package backend

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
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

func NewFileBackend(filename string, datadir string) (fb *FileBackend, err error) {
	fb = &FileBackend{
		filename: filename,
		dataflag: false,
	}

	pathname := filepath.Join(datadir, filename)
	fb.producer, err = os.OpenFile(pathname+".dat", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("open producer error: %s %s", fb.filename, err)
		return
	}

	fb.consumer, err = os.OpenFile(pathname+".dat", os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("open consumer error: %s %s", fb.filename, err)
		return
	}

	fb.meta, err = os.OpenFile(pathname+".rec", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("open meta error: %s %s", fb.filename, err)
		return
	}

	fb.RollbackMeta()
	producerOffset, _ := fb.producer.Seek(0, io.SeekEnd)
	offset, _ := fb.consumer.Seek(0, io.SeekCurrent)
	fb.dataflag = producerOffset > offset
	return
}

func (fb *FileBackend) WriteFile(p []byte) (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	var length = uint32(len(p))
	err = binary.Write(fb.producer, binary.BigEndian, length)
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

func (fb *FileBackend) IsData() bool {
	fb.lock.Lock()
	defer fb.lock.Unlock()
	return fb.dataflag
}

func (fb *FileBackend) ReadFile() (p []byte, err error) {
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

func (fb *FileBackend) RollbackMeta() (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	_, err = fb.meta.Seek(0, io.SeekStart)
	if err != nil {
		log.Printf("seek meta error: %s %s", fb.filename, err)
		return
	}

	var offset int64
	err = binary.Read(fb.meta, binary.BigEndian, &offset)
	if err != nil {
		if err != io.EOF {
			log.Printf("read meta error: %s %s", fb.filename, err)
		}
		return
	}

	_, err = fb.consumer.Seek(offset, io.SeekStart)
	if err != nil {
		log.Printf("seek consumer error: %s %s", fb.filename, err)
		return
	}
	return
}

func (fb *FileBackend) UpdateMeta() (err error) {
	fb.lock.Lock()
	defer fb.lock.Unlock()

	producerOffset, err := fb.producer.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("seek producer error: %s %s", fb.filename, err)
		return
	}

	offset, err := fb.consumer.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("seek consumer error: %s %s", fb.filename, err)
		return
	}

	if producerOffset == offset {
		err = fb.CleanUp()
		if err != nil {
			log.Printf("cleanup error: %s %s", fb.filename, err)
			return
		}
		offset = 0
	}

	_, err = fb.meta.Seek(0, io.SeekStart)
	if err != nil {
		log.Printf("seek meta error: %s %s", fb.filename, err)
		return
	}

	log.Printf("write meta: %s, %d", fb.filename, offset)
	err = binary.Write(fb.meta, binary.BigEndian, &offset)
	if err != nil {
		log.Printf("write meta error: %s %s", fb.filename, err)
		return
	}

	err = fb.meta.Sync()
	if err != nil {
		log.Printf("sync meta error: %s %s", fb.filename, err)
		return
	}

	return
}

func (fb *FileBackend) CleanUp() (err error) {
	_, err = fb.consumer.Seek(0, io.SeekStart)
	if err != nil {
		log.Print("seek consumer error: ", err)
		return
	}
	err = fb.producer.Truncate(0)
	if err != nil {
		log.Print("truncate error: ", err)
		return
	}
	_, err = fb.producer.Seek(0, io.SeekStart)
	if err != nil {
		log.Print("seek producer error: ", err)
		return
	}
	fb.dataflag = false
	return
}

func (fb *FileBackend) Close() {
	fb.producer.Close()
	fb.consumer.Close()
	fb.meta.Close()
}
