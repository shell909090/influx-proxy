package backend

import (
	"bytes"
	"io"
	"log"
	"sync"
	"time"
)

// TODO: cache & redo

type CacheableAPI struct {
	writer   io.Writer
	lock     sync.Mutex
	buffer   *bytes.Buffer
	timer    *time.Timer
	interval int
}

func (ca *CacheableAPI) Flush() {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	p := ca.buffer.Bytes()
	if len(p) == 0 {
		// trigger twice.
		return
	}

	// FIXME: maybe blocked here, should we hold lock now?
	// maybe this can be delayed. Or run in another goroutine?
	n, err := ca.writer.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		log.Printf("error: %s\n", err)
		return
	}

	ca.buffer.Reset()
	ca.timer = nil
	return
}

func (ca *CacheableAPI) Write(p []byte) (n int, err error) {
	ca.lock.Lock()
	defer ca.lock.Unlock()

	n, err = ca.buffer.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		log.Printf("error: %s\n", err)
		return
	}

	// TODO: size > max_size, trigger Flush.
	if ca.timer == nil {
		ca.timer = time.AfterFunc(
			time.Millisecond*time.Duration(ca.interval),
			ca.Flush)
	}

	return
}
