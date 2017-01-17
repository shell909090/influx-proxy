package backend

import (
	"bytes"
	"io"
	"log"
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

type MultiAPI struct {
	lock     sync.RWMutex
	key2apis map[string][]InfluxAPI
}

func (mi *MultiAPI) WriteOneRow(p []byte) (err error) {
	key, err := ScanKey(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	mi.lock.RLock()
	defer mi.lock.Unlock()

	apis, ok := mi.key2apis[key]
	if !ok {
		// TODO: new key?
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
