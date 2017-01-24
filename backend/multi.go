package backend

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"sync"

	"gopkg.in/redis.v5"
)

func ScanKey(pointbuf []byte) (key string, err error) {
	var keybuf [100]byte
	keyslice := keybuf[0:0]
	buflen := len(pointbuf)
	for i := 0; i < buflen; i++ {
		c := pointbuf[i]
		switch c {
		case '\\':
			i++
			keyslice = append(keyslice, pointbuf[i])
		case ' ', ',':
			key = string(keyslice)
			return
		default:
			keyslice = append(keyslice, c)
		}
	}
	return "", io.EOF
}

// faster then bytes.TrimRight, not sure why.
func TrimRight(p []byte, s []byte) (r []byte) {
	r = p
	if len(r) == 0 {
		return
	}

	i := len(r) - 1
	for ; bytes.IndexByte(s, r[i]) != -1; i-- {
	}
	return r[0 : i+1]
}

type MultiAPI struct {
	lock     sync.RWMutex
	Zone     string
	client   *redis.Client
	queue    chan []byte
	upstream map[string]InfluxAPI
	key2apis map[string][]InfluxAPI
}

func NewMultiAPI(client *redis.Client, zone string) (mi *MultiAPI) {
	mi = &MultiAPI{
		Zone:   zone,
		client: client,
		queue:  make(chan []byte, 32),
	}
	// How many workers?
	go mi.worker()
	return
}

func (mi *MultiAPI) loadUpstream() (err error) {
	mi.upstream = make(map[string]InfluxAPI, 1)

	names, err := mi.client.Keys("b:*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var cfg *BackendConfig
	for _, name := range names {
		name = name[2:len(name)]
		cfg, err = LoadConfigFromRedis(mi.client, name)
		if err != nil {
			return
		}
		mi.upstream[name] = cfg.CreateCacheableHttp(name)
	}
	log.Printf("%d upstreams loaded.", len(mi.upstream))
	return
}

func (mi *MultiAPI) loadKeyMap() (key2apis map[string][]InfluxAPI, err error) {
	key2apis = make(map[string][]InfluxAPI, 1)

	measurements, err := mi.client.Keys("m:*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var length int64
	var upstreams []string

	for _, measurement := range measurements {
		measurement = measurement[2:len(measurement)]

		length, err = mi.client.LLen(measurement).Result()
		if err != nil {
			return
		}
		upstreams, err = mi.client.LRange(measurement, 0, length).Result()
		if err != nil {
			return
		}

		var apis []InfluxAPI
		for _, up := range upstreams {
			api, ok := mi.upstream[up]
			if !ok {
				err = ErrIllegalConfig
				log.Fatal(err)
				return
			}
			apis = append(apis, api)
		}
		key2apis[measurement] = apis
	}
	log.Printf("%d measurements loaded.", len(key2apis))
	return
}

func (mi *MultiAPI) LoadConfig() (err error) {
	err = mi.loadUpstream()
	if err != nil {
		return
	}

	key2apis, err := mi.loadKeyMap()
	if err != nil {
		return
	}

	mi.lock.Lock()
	defer mi.lock.Unlock()

	mi.key2apis = key2apis
	// FIXME: free?
	return
}

func (mi *MultiAPI) Ping() (version string, err error) {
	version = VERSION
	return
}

func (mi *MultiAPI) Query(w http.ResponseWriter, req *http.Request) (err error) {
	switch req.Method {
	case "GET", "POST":
	default:
		w.WriteHeader(400)
		w.Write([]byte("illegal method"))
	}

	q := req.URL.Query().Get("q")
	key, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		log.Printf("can't get measurement: %s\n", q)
		w.WriteHeader(400)
		w.Write([]byte("can't get measurement"))
		return
	}

	mi.lock.RLock()
	apis, ok := mi.key2apis[key]
	mi.lock.RUnlock()
	if !ok {
		log.Printf("unknown measurement: %s\n", key)
		w.WriteHeader(400)
		w.Write([]byte("unknown measurement"))
		return
	}

	for _, api := range apis {
		// FIXME:
		// if !api.Active {
		// 	continue
		// }
		err = api.Query(w, req)
		if err == nil {
			return
		}
	}

	return
}

func (mi *MultiAPI) WriteRows(p []byte) (err error) {
	mi.lock.RLock()
	defer mi.lock.RUnlock()
	buf := bytes.NewBuffer(p)

	var line []byte
	var key string
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			log.Printf("error: %s\n", err)
			return
		case io.EOF, nil:
		}

		if len(line) == 0 {
			break
		}

		// maybe trim?
		line = bytes.TrimRight(line, " \t\r\n")

		// empty line, ignore it.
		if len(line) == 0 {
			continue
		}

		key, err = ScanKey(p)
		if err != nil {
			log.Printf("scan key error: %s\n", err)
			// don't stop, try next line.
			continue
		}

		apis, ok := mi.key2apis[key]
		if !ok {
			log.Printf("new measurement: %s\n", key)
			// TODO: new measurement?
			return
		}

		// don't block here for a lont time, we just have one worker.
		for _, api := range apis {
			err = api.Write(p)
			if err != nil {
				// critical
				return
			}
		}
	}

	return
}

func (mi *MultiAPI) worker() {
	for {
		p := <-mi.queue
		err := mi.WriteRows(p)
		if err != nil {
			log.Printf("write row: %s", err)
		}
	}
}

func (mi *MultiAPI) Write(p []byte) (err error) {
	mi.queue <- p
	return
}
