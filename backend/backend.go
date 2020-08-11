package backend

import (
	"bytes"
	"errors"
	"github.com/panjf2000/ants/v2"
	"io"
	"log"
	"net/url"
	"sync"
	"time"
)

var (
	ErrBadRequest   = errors.New("bad request")
	ErrUnauthorized = errors.New("unauthorized")
	ErrNotFound     = errors.New("not found")
	ErrInternal     = errors.New("internal error")
	ErrUnknown      = errors.New("unknown error")
)

type CacheBuffer struct {
	Buffer  *bytes.Buffer
	Counter int
}

type Backend struct {
	*HttpBackend
	fb   *FileBackend
	pool *ants.Pool

	flushSize       int
	flushTime       int
	rewriteInterval int
	rewriteTicker   *time.Ticker
	rewriteRunning  bool

	chWrite   chan *LinePoint
	chTimer   <-chan time.Time
	bufferMap map[string]*CacheBuffer
	wg        sync.WaitGroup
}

func NewBackend(cfg *BackendConfig, pxcfg *ProxyConfig) (backend *Backend) {
	backend = &Backend{
		HttpBackend:     NewHttpBackend(cfg, pxcfg),
		flushSize:       pxcfg.FlushSize,
		flushTime:       pxcfg.FlushTime,
		rewriteInterval: pxcfg.RewriteInterval,
		rewriteTicker:   time.NewTicker(time.Duration(pxcfg.RewriteInterval) * time.Second),
		rewriteRunning:  false,
		chWrite:         make(chan *LinePoint, 16),
		bufferMap:       make(map[string]*CacheBuffer),
	}

	var err error
	backend.fb, err = NewFileBackend(cfg.Name, pxcfg.DataDir)
	if err != nil {
		panic(err)
	}
	backend.pool, err = ants.NewPool(pxcfg.ConnPoolSize)
	if err != nil {
		panic(err)
	}

	go backend.worker()
	return
}

func NewSimpleBackend(cfg *BackendConfig) *Backend {
	return &Backend{HttpBackend: NewSimpleHttpBackend(cfg)}
}

func (backend *Backend) worker() {
	for {
		select {
		case p, ok := <-backend.chWrite:
			if !ok {
				// closed
				backend.Flush()
				backend.wg.Wait()
				backend.HttpBackend.Close()
				backend.fb.Close()
				return
			}
			backend.WriteBuffer(p)

		case <-backend.chTimer:
			backend.Flush()

		case <-backend.rewriteTicker.C:
			backend.RewriteIdle()
		}
	}
}

func (backend *Backend) WritePoint(point *LinePoint) (err error) {
	backend.chWrite <- point
	return
}

func (backend *Backend) WriteBuffer(point *LinePoint) (err error) {
	db, line := point.Db, point.Line
	cb, ok := backend.bufferMap[db]
	if !ok {
		backend.bufferMap[db] = &CacheBuffer{Buffer: &bytes.Buffer{}}
		cb = backend.bufferMap[db]
	}
	cb.Counter++
	if cb.Buffer == nil {
		cb.Buffer = &bytes.Buffer{}
	}
	n, err := cb.Buffer.Write(line)
	if err != nil {
		log.Printf("buffer write error: %s\n", err)
		return
	}
	if n != len(line) {
		err = io.ErrShortWrite
		log.Printf("buffer write error: %s\n", err)
		return
	}
	if line[len(line)-1] != '\n' {
		_, err = cb.Buffer.Write([]byte{'\n'})
		if err != nil {
			log.Printf("buffer write error: %s\n", err)
			return
		}
	}

	switch {
	case cb.Counter >= backend.flushSize:
		err = backend.FlushBuffer(db)
		if err != nil {
			return
		}
	case backend.chTimer == nil:
		backend.chTimer = time.After(time.Duration(backend.flushTime) * time.Second)
	}
	return
}

func (backend *Backend) FlushBuffer(db string) (err error) {
	cb := backend.bufferMap[db]
	if cb.Buffer == nil {
		return
	}
	p := cb.Buffer.Bytes()
	cb.Buffer = nil
	cb.Counter = 0
	if len(p) == 0 {
		return
	}

	backend.wg.Add(1)
	backend.pool.Submit(func() {
		defer backend.wg.Done()
		var buf bytes.Buffer
		err = Compress(&buf, p)
		if err != nil {
			log.Print("compress buffer error: ", err)
			return
		}

		p = buf.Bytes()

		if backend.Active {
			err = backend.WriteCompressed(db, p)
			switch err {
			case nil:
				return
			case ErrBadRequest:
				log.Printf("bad request, drop all data")
				return
			case ErrNotFound:
				log.Printf("bad backend, drop all data")
				return
			default:
				log.Printf("write http error: %s %s, length: %d", backend.Url, db, len(p))
			}
		}

		b := bytes.Join([][]byte{[]byte(url.QueryEscape(db)), p}, []byte{' '})
		err = backend.fb.WriteFile(b)
		if err != nil {
			log.Printf("write db and data to file error with db: %s, length: %d error: %s", db, len(p), err)
			return
		}
	})
	return
}

func (backend *Backend) Flush() {
	backend.chTimer = nil
	for db := range backend.bufferMap {
		if backend.bufferMap[db].Counter > 0 {
			err := backend.FlushBuffer(db)
			if err != nil {
				log.Printf("flush buffer background error: %s %s", backend.Url, err)
			}
		}
	}
}

func (backend *Backend) RewriteIdle() {
	if !backend.rewriteRunning && backend.fb.IsData() {
		backend.rewriteRunning = true
		go backend.RewriteLoop()
	}
}

func (backend *Backend) RewriteLoop() {
	for backend.fb.IsData() {
		if !backend.Active {
			time.Sleep(time.Duration(backend.rewriteInterval) * time.Second)
			continue
		}
		err := backend.Rewrite()
		if err != nil {
			time.Sleep(time.Duration(backend.rewriteInterval) * time.Second)
			continue
		}
	}
	backend.rewriteRunning = false
}

func (backend *Backend) Rewrite() (err error) {
	b, err := backend.fb.ReadFile()
	if err != nil {
		log.Print("rewrite read file error: ", err)
		return
	}
	if b == nil {
		return
	}

	p := bytes.SplitN(b, []byte{' '}, 2)
	if len(p) < 2 {
		log.Print("rewrite read invalid data with length: ", len(p))
		return
	}
	db, err := url.QueryUnescape(string(p[0]))
	if err != nil {
		log.Print("rewrite db unescape error: ", err)
		return
	}
	err = backend.WriteCompressed(db, p[1])

	switch err {
	case nil:
	case ErrBadRequest:
		log.Printf("bad request, drop all data")
		err = nil
	case ErrNotFound:
		log.Printf("bad backend, drop all data")
		err = nil
	default:
		log.Printf("rewrite http error: %s %s, length: %d", backend.Url, db, len(p[1]))

		err = backend.fb.RollbackMeta()
		if err != nil {
			log.Printf("rollback meta error: %s", err)
		}
		return
	}

	err = backend.fb.UpdateMeta()
	if err != nil {
		log.Printf("update meta error: %s", err)
	}
	return
}

func (backend *Backend) RewriteRunning() bool {
	return backend.rewriteRunning
}

func (backend *Backend) Close() {
	backend.pool.Release()
	close(backend.chWrite)
}
