package backend

import (
	"bytes"
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
	fb       *FileBackend
	Interval int
	Timeout  int

	running          bool
	ticker           *time.Ticker
	ch_write         chan []byte
	buffer           *bytes.Buffer
	ch_timer         <-chan time.Time
	write_counter    int32
	rewriter_running bool
}

// maybe ch_timer is not the best way.
func NewBackends(cfg *BackendConfig, name string) (bs *Backends, err error) {
	bs = &Backends{
		HttpBackend: NewHttpBackend(cfg),
		// FIXME: path...
		Interval: cfg.Interval,
		Timeout:  cfg.Timeout,
		running:  true,
		ticker:   time.NewTicker(time.Millisecond * time.Duration(cfg.Timeout)),
		ch_write: make(chan []byte, 16),

		rewriter_running: false,
	}
	bs.fb, err = NewFileBackend(name)
	if err != nil {
		return
	}

	go bs.worker()
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

		case <-bs.ticker.C:
			bs.Idle()
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

func (bs *Backends) WriteBuffer(p []byte) {
	bs.write_counter++

	if bs.buffer == nil {
		bs.buffer = &bytes.Buffer{}
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
		_, err = bs.buffer.Write([]byte{'\n'})
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

func (bs *Backends) Flush() {
	if bs.buffer == nil {
		return
	}

	p := bs.buffer.Bytes()
	bs.buffer = nil
	bs.ch_timer = nil

	if len(p) == 0 {
		return
	}

	// TODO: limitation
	go func() {
		var buf bytes.Buffer
		err := Compress(&buf, p)
		if err != nil {
			log.Printf("write file error: %s\n", err)
			return
		}

		p = buf.Bytes()

		// maybe blocked here, run in another goroutine
		if bs.HttpBackend.IsActive() {
			err = bs.HttpBackend.WriteCompressed(p)
			if err == nil {
				return
			}
			log.Printf("write http error: %s\n", err)
		}

		err = bs.fb.Write(p)
		if err != nil {
			log.Printf("write file error: %s\n", err)
		}
		// don't try to run rewrite loop directly.
		// that need a lock.
	}()

	return
}

func (bs *Backends) Idle() {
	if !bs.rewriter_running && bs.fb.IsData() {
		bs.rewriter_running = true
		go bs.RewriteLoop()
	}

	// TODO: report counter
}

func (bs *Backends) RewriteLoop() {
	for bs.fb.IsData() {
		if !bs.HttpBackend.IsActive() {
			time.Sleep(time.Millisecond * time.Duration(bs.Timeout))
			continue
		}
		err := bs.Rewrite()
		if err != nil {
			time.Sleep(time.Millisecond * time.Duration(bs.Timeout))
			continue
		}
	}
	bs.rewriter_running = false
}

func (bs *Backends) Rewrite() (err error) {
	p, err := bs.fb.Read()
	if err != nil {
		return
	}
	if p == nil { // why?
		return
	}

	err = bs.HttpBackend.WriteCompressed(p)
	if err != nil {
		log.Printf("rewrite http error: %s\n", err)

		err = bs.fb.RollbackMeta()
		if err != nil {
			log.Printf("rollback meta error: %s\n", err)
		}
		return
	}

	err = bs.fb.UpdateMeta()
	if err != nil {
		log.Printf("update meta error: %s\n", err)
		return
	}
	return
}
