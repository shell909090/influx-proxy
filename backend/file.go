package backend

import (
	"io"
	"os"
)

type FileBackend struct {
	hb   *HttpBackend
	data *os.File
}

func NewFileBackend(hb *HttpBackend, filename string) (fb *FileBackend, err error) {
	fb := &FileBackend{
		hb: hb,
	}
	fb.data, err = os.OpenFile(filename+".dat", os.O_RDWR|os.O_APPEND|O_CREATE, 0666)
	if err != nil {
		return
	}
	return
}

func (fb *FileBackend) Write(p []byte) (err error) {
	n, err := fb.Write(p)
	if err != nil {
		return
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	return
}
