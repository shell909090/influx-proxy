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

func GetMeasurementFromInfluxQL(q string) (m string, err error) {
	return
}

type MultiAPI struct {
	lock     sync.RWMutex
	key2apis map[string][]InfluxAPI
}

func NewMultiAPI(key2apis map[string][]InfluxAPI) (mi *MultiAPI) {
	mi = &MultiAPI{
		key2apis: key2apis,
	}
	return
}

func (mi *MultiAPI) Ping() (version string, err error) {
	version = VERSION
	return
}

// FIXME:
func (mi *MultiAPI) Query(w http.ResponseWriter, req *http.Request) (err error) {
	switch req.Method {
	case "GET", "POST":
	default:
		w.WriteHeader(400)
		w.Write([]byte("illegal method"))
	}

	// q := req.URL.Query().Get("q")

	return
}

func (mi *MultiAPI) WriteOneRow(p []byte) (err error) {
	key, err := ScanKey(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	mi.lock.RLock()
	defer mi.lock.RUnlock()

	apis, ok := mi.key2apis[key]
	if !ok {
		log.Printf("new measurement: %s\n", key)
		// TODO: new measurement?
		return
	}

	for _, api := range apis {
		// TODO: blocked?
		err = api.Write(p)
		if err != nil {
			// critical
			return
		}
	}

	return
}

func (mi *MultiAPI) Write(p []byte) (err error) {
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

		line = bytes.TrimRight(line, " \t\r\n")
		err = mi.WriteOneRow(line)
		if err != nil {
			log.Printf("error: %s\n", err)
			return
		}
	}

	return
}
