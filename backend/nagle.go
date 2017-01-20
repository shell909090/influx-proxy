package backend

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"
)

// TODO: redo

type CacheableAPI struct {
	InfluxAPI
	Interval int

	lock   sync.Mutex
	buffer bytes.Buffer
	timer  *time.Timer
}

func NewCacheableAPI(api InfluxAPI, cfg *BackendConfig) (ca *CacheableAPI) {
	ca = &CacheableAPI{
		InfluxAPI: api,
		Interval:  cfg.Interval,
	}
	return
}

func (ca *CacheableAPI) Flush() {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	p := ca.buffer.Bytes()
	if len(p) == 0 {
		// trigger twice.
		return
	}
	ca.buffer.Reset()
	ca.timer = nil

	go func() {
		// maybe blocked here, run in another goroutine
		err := ca.InfluxAPI.Write(p)
		if err != nil {
			log.Printf("error: %s\n", err)
			return
		}
	}()

	return
}

func (ca *CacheableAPI) Write(p []byte) (err error) {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	n, err := ca.buffer.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		log.Printf("error: %s\n", err)
		return
	}

	err = ca.buffer.WriteByte('\n')
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	if ca.timer == nil {
		ca.timer = time.AfterFunc(
			time.Millisecond*time.Duration(ca.Interval),
			ca.Flush)
	}

	return
}
