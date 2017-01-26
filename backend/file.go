package backend

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"sync"
)

type FileBackend struct {
	lock     sync.Mutex
	filename string
	dataflag bool
	producer *os.File
	consumer *os.File
	meta     *os.File
}

func NewFileBackend(filename string) (fb *FileBackend, err error) {
	fb = &FileBackend{
		filename: filename,
		dataflag: false,
	}

	fb.producer, err = os.OpenFile(filename+".dat",
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Print("open producer error: ", err)
		return
	}

	fb.consumer, err = os.OpenFile(filename+".dat",
		os.O_RDONLY, 0644)
	if err != nil {
		log.Print("open consumer error: ", err)
		return
	}

	fb.meta, err = os.OpenFile(filename+".rec",
		os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Print("open meta error: ", err)
		return
	}

	err = fb.RollbackMeta()
	if err != nil {
		err = nil
	}
	return
}

func (fb *FileBackend) Write(p []byte) (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	var length uint32 = uint32(len(p))
	binary.Write(fb.producer, binary.BigEndian, length)
	if err != nil {
		log.Print("write length error: ", err)
		return
	}

	n, err := fb.producer.Write(p)
	if err != nil {
		log.Print("write error: ", err)
		return
	}
	if n != len(p) {
		return io.ErrShortWrite
	}

	err = fb.producer.Sync()
	if err != nil {
		log.Print("sync meta error: ", err)
		return
	}

	fb.dataflag = true
	return
}

func (fb *FileBackend) IsData() (dataflag bool) {
	fb.lock.Lock()
	defer fb.lock.Unlock()
	return fb.dataflag
}

// FIXME: signal here
func (fb *FileBackend) Read() (p []byte, err error) {
	if !fb.IsData() {
		return nil, nil
	}

	var length uint32

	err = binary.Read(fb.consumer, binary.BigEndian, &length)
	if err != nil {
		log.Print("read length error: ", err)
		return
	}

	p = make([]byte, length)

	_, err = io.ReadFull(fb.consumer, p)
	if err != nil {
		log.Print("read error: ", err)
		return
	}
	return
}

func (fb *FileBackend) CleanUp() (err error) {
	_, err = fb.consumer.Seek(0, os.SEEK_SET)
	if err != nil {
		log.Print("seek consumer error: ", err)
		return
	}

	err = fb.producer.Truncate(0)
	if err != nil {
		log.Print("truncate error: ", err)
		return
	}

	err = fb.producer.Close()
	if err != nil {
		log.Print("close producer error: ", err)
		return
	}

	fb.producer, err = os.OpenFile(fb.filename+".dat",
		os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Print("open producer error: ", err)
		return
	}

	fb.dataflag = false
	return
}

func (fb *FileBackend) UpdateMeta() (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	off_producer, err := fb.producer.Seek(0, os.SEEK_CUR)
	if err != nil {
		log.Print("OK")
		log.Print("seek producer error: ", err)
		return
	}

	off, err := fb.consumer.Seek(0, os.SEEK_CUR)
	if err != nil {
		log.Print("seek consumer error: ", err)
		return
	}

	if off_producer == off {
		err = fb.CleanUp()
		if err != nil {
			return
		}
		off = 0
	}

	_, err = fb.meta.Seek(0, os.SEEK_SET)
	if err != nil {
		log.Print("seek meta error: ", err)
		return
	}

	log.Printf("write meta: %d", off)
	err = binary.Write(fb.meta, binary.BigEndian, &off)
	if err != nil {
		log.Print("write meta error: ", err)
		return
	}

	err = fb.meta.Sync()
	if err != nil {
		log.Print("sync meta error: ", err)
		return
	}

	return
}

func (fb *FileBackend) RollbackMeta() (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	_, err = fb.meta.Seek(0, os.SEEK_SET)
	if err != nil {
		log.Print("seek meta error: ", err)
		return
	}

	var off int64
	err = binary.Read(fb.meta, binary.BigEndian, &off)
	if err != nil {
		log.Print("read meta error: ", err)
		return
	}

	_, err = fb.consumer.Seek(off, os.SEEK_SET)
	if err != nil {
		log.Print("seek consumer error: ", err)
		return
	}
	return
}
