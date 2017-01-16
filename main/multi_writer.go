package main

import (
	"sync"
	"io"
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

type MultiWriter struct {
	lock sync.RWMutex
	key2writers map[string][]io.Writer
}

func (mw *MultiWriter) Write(p []byte) (n int, err error) {
	key, err := ScanKey(p)
	if err != nil {
		// FIXME:
		return
	}

	mw.lock.RLock()
	defer mw.lock.Unlock()

	writers, ok := mw.key2writers[key]
	if !ok {
		// TODO: new key?
		return
	}

	for _, writer := range writers {
		n, err = writer.Write(p)
		if err != nil {
			// critical
			return
		}
	}

	n := len(p)
	return
}
