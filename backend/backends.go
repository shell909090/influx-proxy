package backend

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"
)

// TODO: redo

type Backends struct {
	InfluxAPI
	Interval int

	lock   sync.Mutex
	buffer bytes.Buffer
	timer  *time.Timer
}

func NewBackends(cfg *BackendConfig, name string) (bs *Backends) {
	bs = &Backends{
		InfluxAPI: NewHttpBackend(cfg),
		Interval:  cfg.Interval,
	}
	return
}

// TODO: move compress here
func (bs *Backends) Flush() {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	p := bs.buffer.Bytes()
	if len(p) == 0 {
		// trigger twice.
		return
	}
	bs.buffer.Reset()
	bs.timer = nil

	go func() {
		// maybe blocked here, run in another goroutine
		err := bs.InfluxAPI.Write(p)
		if err != nil {
			log.Printf("error: %s\n", err)
			return
		}
	}()

	return
}

// TODO: add counter
func (bs *Backends) Write(p []byte) (err error) {
	bs.lock.Lock()
	defer bs.lock.Unlock()

	n, err := bs.buffer.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		log.Printf("error: %s\n", err)
		return
	}

	if p[len(p)-1] != '\n' {
		err = bs.buffer.WriteByte('\n')
		if err != nil {
			log.Printf("error: %s\n", err)
			return
		}
	}

	if bs.timer == nil {
		bs.timer = time.AfterFunc(
			time.Millisecond*time.Duration(bs.Interval),
			bs.Flush)
	}

	return
}
