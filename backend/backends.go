package backend

import (
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"time"
)

// TODO: redo

const (
	WRITE_QUEUE = 16
)

type Backends struct {
	*HttpBackend
	Interval int

	running       bool
	ch_write      chan []byte
	buffer        *bytes.Buffer
	writer        *gzip.Writer
	ch_timer      <-chan time.Time
	write_counter int32
}

// maybe ch_timer is not the best way.
func NewBackends(cfg *BackendConfig, name string) (bs *Backends) {
	bs = &Backends{
		HttpBackend: NewHttpBackend(cfg),
		Interval:    cfg.Interval,
		running:     true,
		ch_write:    make(chan []byte, 16),
	}
	go bs.worker()
	return
}

func (bs *Backends) Flush() {
	if bs.buffer == nil {
		return
	}

	err := bs.writer.Close()
	if err != nil {
		log.Printf("zip close error: %s\n", err)
		return
	}

	p := bs.buffer.Bytes()
	bs.buffer.Reset()
	bs.buffer = nil
	bs.writer = nil
	bs.ch_timer = nil

	if len(p) == 0 {
		return
	}

	// maybe blocked here, run in another goroutine
	err = bs.HttpBackend.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	return
}

// TODO: add counter
// FIXME: move compress here
func (bs *Backends) WriteBuffer(p []byte) {
	bs.write_counter++

	if bs.buffer == nil {
		bs.buffer = &bytes.Buffer{}
		bs.writer = gzip.NewWriter(bs.buffer)
	}

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

	if bs.ch_timer == nil {
		bs.ch_timer = time.After(time.Millisecond * time.Duration(bs.Interval))
	}

	return
}

func (bs *Backends) worker() {
	for bs.running {
		select {
		case p, ok := <-bs.ch_write:
			if !ok {
				// closed
				bs.Flush()
				bs.HttpBackend.Close()
				return
			}
			bs.WriteBuffer(p)

		case <-bs.ch_timer:
			bs.Flush()
			if !bs.running {
				bs.HttpBackend.Close()
				return
			}
		}
	}
}

func (bs *Backends) Write(p []byte) (err error) {
	if !bs.running {
		return io.ErrClosedPipe
	}

	bs.ch_write <- p
	return
}

func (bs *Backends) Close() (err error) {
	bs.running = false
	close(bs.ch_write)
	return
}
