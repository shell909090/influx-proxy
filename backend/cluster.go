package backend

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"

	redis "gopkg.in/redis.v5"
)

var (
	ErrClosed = errors.New("write in a closed file")
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

type InfluxCluster struct {
	lock     sync.RWMutex
	Zone     string
	client   *redis.Client
	backends map[string]BackendAPI
	// measurements to backends
	m2bs map[string][]BackendAPI
}

func NewInfluxCluster(client *redis.Client, zone string) (ic *InfluxCluster) {
	ic = &InfluxCluster{
		Zone:   zone,
		client: client,
	}
	return
}

func (ic *InfluxCluster) loadBackends() (backends map[string]BackendAPI, err error) {
	backends = make(map[string]BackendAPI)

	names, err := ic.client.Keys("b:*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var cfg *BackendConfig
	for _, name := range names {
		name = name[2:len(name)]
		cfg, err = LoadConfigFromRedis(ic.client, name)
		if err != nil {
			return
		}
		backends[name] = NewBackends(cfg, name)
	}
	log.Printf("%d backends loaded.", len(backends))
	return
}

func (ic *InfluxCluster) loadMeasurements(backends map[string]BackendAPI) (m2bs map[string][]BackendAPI, err error) {
	m2bs = make(map[string][]BackendAPI)

	m_names, err := ic.client.Keys("m:*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var length int64
	var bs_names []string

	for _, key := range m_names {
		length, err = ic.client.LLen(key).Result()
		if err != nil {
			return
		}
		bs_names, err = ic.client.LRange(key, 0, length).Result()
		if err != nil {
			return
		}

		var bss []BackendAPI
		for _, bs_name := range bs_names {
			bs, ok := backends[bs_name]
			if !ok {
				err = ErrIllegalConfig
				log.Fatal(err)
				return
			}
			bss = append(bss, bs)
		}
		m2bs[key[2:len(key)]] = bss
	}
	log.Printf("%d measurements loaded.", len(m2bs))
	return
}

func (ic *InfluxCluster) LoadConfig() (err error) {
	backends, err := ic.loadBackends()
	if err != nil {
		return
	}

	m2bs, err := ic.loadMeasurements(backends)
	if err != nil {
		return
	}

	ic.lock.Lock()
	orig_backends := ic.backends
	ic.backends = backends
	ic.m2bs = m2bs
	ic.lock.Unlock()

	for name, bs := range orig_backends {
		err = bs.Close()
		if err != nil {
			log.Printf("fail in close backend %s", name)
		}
	}
	return
}

func (ic *InfluxCluster) Ping() (version string, err error) {
	version = VERSION
	return
}

func (ic *InfluxCluster) Query(w http.ResponseWriter, req *http.Request) (err error) {
	switch req.Method {
	case "GET", "POST":
	default:
		w.WriteHeader(400)
		w.Write([]byte("illegal method"))
		return
	}

	// TODO: all query in q?
	q := req.URL.Query().Get("q")
	if q == "" {
		w.WriteHeader(400)
		w.Write([]byte("empty query"))
		return
	}

	key, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		log.Printf("can't get measurement: %s\n", q)
		w.WriteHeader(400)
		w.Write([]byte("can't get measurement"))
		return
	}

	ic.lock.RLock()
	apis, ok := ic.m2bs[key]
	ic.lock.RUnlock()
	if !ok {
		log.Printf("unknown measurement: %s\n", key)
		w.WriteHeader(400)
		w.Write([]byte("unknown measurement"))
		return
	}

	// same zone first, other zone. pass non-active.
	// TODO: better way?

	for _, api := range apis {
		if api.GetZone() != ic.Zone {
			continue
		}
		if !api.IsActive() {
			continue
		}
		err = api.Query(w, req)
		if err == nil {
			return
		}
	}

	for _, api := range apis {
		if api.GetZone() == ic.Zone {
			continue
		}
		if !api.IsActive() {
			continue
		}
		err = api.Query(w, req)
		if err == nil {
			return
		}
	}

	return
}

// Wrong in one row will not stop others.
// So don't try to return error, just print it.
func (ic *InfluxCluster) WriteRow(line []byte) {
	// maybe trim?
	line = bytes.TrimRight(line, " \t\r\n")

	// empty line, ignore it.
	if len(line) == 0 {
		return
	}

	key, err := ScanKey(line)
	if err != nil {
		log.Printf("scan key error: %s\n", err)
		return
	}

	ic.lock.RLock()
	bs, ok := ic.m2bs[key]
	ic.lock.RUnlock()
	if !ok {
		log.Printf("new measurement: %s\n", key)
		// TODO: new measurement?
		return
	}

	// don't block here for a lont time, we just have one worker.
	for _, b := range bs {
		err = b.Write(line)
		if err != nil {
			log.Printf("cluster write fail: %s\n", key)
			return
		}
	}
	return
}

func (ic *InfluxCluster) Write(p []byte) (err error) {
	buf := bytes.NewBuffer(p)

	var line []byte
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

		ic.WriteRow(line)
	}

	return
}

func (ic *InfluxCluster) Close() (err error) {
	return
}
