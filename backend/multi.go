package backend

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"sync"
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
	queue    chan []byte
	key2apis map[string][]InfluxAPI
}

func NewMultiAPI(key2apis map[string][]InfluxAPI) (mi *MultiAPI) {
	mi = &MultiAPI{
		queue:    make(chan []byte, 32),
		key2apis: key2apis,
	}
	// How many workers?
	go mi.worker()
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
