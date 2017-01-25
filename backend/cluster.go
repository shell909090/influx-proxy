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

type InfluxCluster struct {
	lock     sync.RWMutex
	Zone     string
	client   *redis.Client
	queue    chan []byte
	running  bool
	backends map[string]InfluxAPI
	// measurements to backends
	m2bs     map[string][]InfluxAPI
}

func NewInfluxCluster(client *redis.Client, zone string) (ic *InfluxCluster) {
	ic = &InfluxCluster{
		Zone:    zone,
		client:  client,
		queue:   make(chan []byte, 32),
		running: true,
	}
	// How many workers?
	go ic.worker()
	return
}

func (ic *InfluxCluster) loadBackends() (backends map[string]InfluxAPI, err error) {
	backends = make(map[string]InfluxAPI)

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

func (ic *InfluxCluster) loadMeasurements(backends map[string]InfluxAPI) (m2bs map[string][]InfluxAPI, err error) {
	m2bs = make(map[string][]InfluxAPI)

	m_names, err := ic.client.Keys("m:*").Result()
	if err != nil {
		log.Printf("read redis error: %s", err)
		return
	}

	var length int64
	var bs_names []string

	for _, m_name := range m_names {
		m_name = m_name[2:len(m_name)]

		length, err = ic.client.LLen(m_name).Result()
		if err != nil {
			return
		}
		bs_names, err = ic.client.LRange(m_name, 0, length).Result()
		if err != nil {
			return
		}

		var bss []InfluxAPI
		for _, bs_name := range bs_names {
			bs, ok := backends[bs_name]
			if !ok {
				err = ErrIllegalConfig
				log.Fatal(err)
				return
			}
			bss = append(bss, bs)
		}
		m2bs[m_name] = bss
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

func (ic *InfluxCluster) WriteRows(p []byte) (err error) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()
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

		bs, ok := ic.m2bs[key]
		if !ok {
			log.Printf("new measurement: %s\n", key)
			// TODO: new measurement?
			return
		}

		// don't block here for a lont time, we just have one worker.
		for _, b := range bs {
			err = b.Write(p)
			if err != nil {
				// critical
				return
			}
		}
	}

	return
}

func (ic *InfluxCluster) worker() {
	for ic.running {
		p := <-ic.queue
		err := ic.WriteRows(p)
		if err != nil {
			log.Printf("write row: %s", err)
		}
	}
}

func (ic *InfluxCluster) Write(p []byte) (err error) {
	// Sometimes, 'if running' go first, then 'running = false' and 'close queue'.
	// So 'send queue' will panic.
	// Thanks god, when this really happen, program should stop soon.
	// In the other hand, why use call write after close?
	if !ic.running {
		return ErrClosed
	}
	ic.queue <- p
	return
}

func (ic *InfluxCluster) Close () (err error) {
	ic.running = false
	close(ic.queue)
	return
}
