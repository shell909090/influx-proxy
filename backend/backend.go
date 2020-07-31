package backend

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/chengshiwen/influx-proxy/util"
	gzip "github.com/klauspost/pgzip"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
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
	Buffer  *bytes.Buffer `json:"buffer"`
	Counter int           `json:"counter"`
}

type Backend struct {
	Name            string                  `json:"name"`
	Url             string                  `json:"url"`
	Username        string                  `json:"username"`
	Password        string                  `json:"password"`
	AuthSecure      bool                    `json:"auth_secure"`
	FlushSize       int                     `json:"flush_size"`
	FlushTime       int                     `json:"flush_time"`
	CheckInterval   int                     `json:"check_interval"`
	RewriteInterval int                     `json:"rewrite_interval"`
	RewriteTicker   *time.Ticker            `json:"rewrite_ticker"`
	RewriteRunning  bool                    `json:"rewrite_running"`
	DataFlag        bool                    `json:"data_flag"`
	Producer        *os.File                `json:"producer"`
	Consumer        *os.File                `json:"consumer"`
	Meta            *os.File                `json:"meta"`
	Client          *http.Client            `json:"client"`
	Transport       *http.Transport         `json:"transport"`
	Active          bool                    `json:"active"`
	ChWrite         chan *LineData          `json:"ch_write"`
	ChTimer         <-chan time.Time        `json:"ch_timer"`
	BufferMap       map[string]*CacheBuffer `json:"buffer_map"`
	Lock            *sync.RWMutex           `json:"lock"`
}

// handle file

func (backend *Backend) OpenFile(dataDir string) {
	var err error
	filename := filepath.Join(dataDir, backend.Name)
	backend.Producer, err = os.OpenFile(filename+".dat", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("open producer error: %s %s", backend.Url, err)
		panic(err)
	}
	producerOffset, err := backend.Producer.Seek(0, io.SeekEnd)
	if err != nil {
		log.Printf("seek producer error: %s %s", backend.Url, err)
		panic(err)
	}

	backend.Consumer, err = os.OpenFile(filename+".dat", os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("open consumer error: %s %s", backend.Url, err)
		panic(err)
	}

	backend.Meta, err = os.OpenFile(filename+".rec", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("open meta error: %s %s", backend.Url, err)
		panic(err)
	}

	backend.RollbackMeta()
	offset, _ := backend.Consumer.Seek(0, io.SeekCurrent)
	backend.DataFlag = producerOffset > offset
}

func (backend *Backend) WriteFile(p []byte) (err error) {
	backend.Lock.Lock()
	defer backend.Lock.Unlock()

	var length = uint32(len(p))
	err = binary.Write(backend.Producer, binary.BigEndian, length)
	if err != nil {
		log.Print("write length error: ", err)
		return
	}

	n, err := backend.Producer.Write(p)
	if err != nil {
		log.Print("write error: ", err)
		return
	}
	if n != len(p) {
		return io.ErrShortWrite
	}

	err = backend.Producer.Sync()
	if err != nil {
		log.Print("sync meta error: ", err)
		return
	}

	backend.DataFlag = true
	return
}

func (backend *Backend) IsData() bool {
	backend.Lock.Lock()
	defer backend.Lock.Unlock()
	return backend.DataFlag
}

func (backend *Backend) ReadFile() (p []byte, err error) {
	if !backend.IsData() {
		return nil, nil
	}
	var length uint32

	err = binary.Read(backend.Consumer, binary.BigEndian, &length)
	if err != nil {
		log.Print("read length error: ", err)
		return
	}
	p = make([]byte, length)

	_, err = io.ReadFull(backend.Consumer, p)
	if err != nil {
		log.Print("read error: ", err)
		return
	}
	return
}

func (backend *Backend) RollbackMeta() (err error) {
	backend.Lock.Lock()
	defer backend.Lock.Unlock()

	_, err = backend.Meta.Seek(0, io.SeekStart)
	if err != nil {
		log.Printf("seek meta error: %s %s", backend.Url, err)
		return
	}

	var offset int64
	err = binary.Read(backend.Meta, binary.BigEndian, &offset)
	if err != nil {
		if err != io.EOF {
			log.Printf("read meta error: %s %s", backend.Url, err)
		}
		return
	}

	_, err = backend.Consumer.Seek(offset, io.SeekStart)
	if err != nil {
		log.Printf("seek consumer error: %s %s", backend.Url, err)
		return
	}
	return
}

func (backend *Backend) UpdateMeta() (err error) {
	backend.Lock.Lock()
	defer backend.Lock.Unlock()

	producerOffset, err := backend.Producer.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("seek producer error: %s %s", backend.Url, err)
		return
	}

	offset, err := backend.Consumer.Seek(0, io.SeekCurrent)
	if err != nil {
		log.Printf("seek consumer error: %s %s", backend.Url, err)
		return
	}

	if producerOffset == offset {
		err = backend.CleanUp()
		if err != nil {
			log.Printf("cleanup error: %s %s", backend.Url, err)
			return
		}
		offset = 0
	}

	_, err = backend.Meta.Seek(0, io.SeekStart)
	if err != nil {
		log.Printf("seek meta error: %s %s", backend.Url, err)
		return
	}

	log.Printf("write meta: %s, %d", backend.Url, offset)
	err = binary.Write(backend.Meta, binary.BigEndian, &offset)
	if err != nil {
		log.Printf("write meta error: %s %s", backend.Url, err)
		return
	}

	err = backend.Meta.Sync()
	if err != nil {
		log.Printf("sync meta error: %s %s", backend.Url, err)
		return
	}

	return
}

func (backend *Backend) CleanUp() (err error) {
	_, err = backend.Consumer.Seek(0, io.SeekStart)
	if err != nil {
		log.Print("seek consumer error: ", err)
		return
	}
	err = backend.Producer.Truncate(0)
	if err != nil {
		log.Print("truncate error: ", err)
		return
	}
	_, err = backend.Producer.Seek(0, io.SeekStart)
	if err != nil {
		log.Print("seek producer error: ", err)
		return
	}
	backend.DataFlag = false
	return
}

func (backend *Backend) Close() {
	backend.Producer.Close()
	backend.Consumer.Close()
	backend.Meta.Close()
	close(backend.ChWrite)
}

// handle write

func (backend *Backend) Worker() {
	for {
		select {
		case data, ok := <-backend.ChWrite:
			if !ok {
				// closed
				backend.Flush()
				backend.Close()
				return
			}
			backend.WriteBuffer(data)

		case <-backend.ChTimer:
			backend.Flush()

		case <-backend.RewriteTicker.C:
			backend.RewriteIdle()
		}
	}
}

func (backend *Backend) WriteData(data *LineData) (err error) {
	backend.ChWrite <- data
	return
}

func (backend *Backend) WriteBuffer(data *LineData) (err error) {
	db := data.Db
	cb, ok := backend.BufferMap[db]
	if !ok {
		backend.BufferMap[db] = &CacheBuffer{Buffer: &bytes.Buffer{}}
		cb = backend.BufferMap[db]
	}
	cb.Counter++
	n, err := cb.Buffer.Write(data.Line)
	if err != nil {
		log.Printf("buffer write error: %s\n", err)
		return
	}
	if n != len(data.Line) {
		err = io.ErrShortWrite
		log.Printf("buffer write error: %s\n", err)
		return
	}

	switch {
	case cb.Counter >= backend.FlushSize:
		err = backend.FlushBuffer(db)
		if err != nil {
			return
		}
	case backend.ChTimer == nil:
		backend.ChTimer = time.After(time.Duration(backend.FlushTime) * time.Second)
	}
	return
}

func (backend *Backend) FlushBuffer(db string) (err error) {
	cb := backend.BufferMap[db]
	p := cb.Buffer.Bytes()
	cb.Buffer.Reset()
	cb.Counter = 0
	if len(p) == 0 {
		return
	}

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

	b := bytes.Join([][]byte{[]byte(url.QueryEscape(db)), p}, []byte(" "))
	err = backend.WriteFile(b)
	if err != nil {
		log.Printf("write db and data to file error with db: %s, length: %d error: %s", db, len(p), err)
		return
	}
	return
}

func (backend *Backend) Flush() {
	backend.ChTimer = nil
	for db := range backend.BufferMap {
		if backend.BufferMap[db].Counter > 0 {
			err := backend.FlushBuffer(db)
			if err != nil {
				log.Printf("flush buffer background error: %s %s", backend.Url, err)
			}
		}
	}
}

func (backend *Backend) RewriteIdle() {
	if !backend.RewriteRunning && backend.IsData() {
		backend.RewriteRunning = true
		go backend.RewriteLoop()
	}
}

func (backend *Backend) RewriteLoop() {
	for backend.IsData() {
		if !backend.Active {
			time.Sleep(time.Duration(backend.RewriteInterval) * time.Second)
			continue
		}
		err := backend.Rewrite()
		if err != nil {
			time.Sleep(time.Duration(backend.RewriteInterval) * time.Second)
			continue
		}
	}
	backend.RewriteRunning = false
}

func (backend *Backend) Rewrite() (err error) {
	b, err := backend.ReadFile()
	if err != nil {
		log.Print("rewrite read file error: ", err)
		return
	}
	if b == nil {
		return
	}

	p := bytes.SplitN(b, []byte(" "), 2)
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

		err = backend.RollbackMeta()
		if err != nil {
			log.Printf("rollback meta error: %s", err)
		}
		return
	}

	err = backend.UpdateMeta()
	if err != nil {
		log.Printf("update meta error: %s", err)
	}
	return
}

// handle http

func NewClient(tlsSkip bool, timeout int) *http.Client {
	return &http.Client{Transport: NewTransport(tlsSkip), Timeout: time.Duration(timeout) * time.Second}
}

func NewTransport(tlsSkip bool) *http.Transport {
	return &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: tlsSkip}}
}

func Compress(buf *bytes.Buffer, p []byte) (err error) {
	zip := gzip.NewWriter(buf)
	defer zip.Close()
	n, err := zip.Write(p)
	if err != nil {
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		return
	}
	return
}

func NewRequest(db, query string) *http.Request {
	header := map[string][]string{"Accept-Encoding": {"gzip"}}
	if db == "" {
		return &http.Request{Form: url.Values{"q": []string{query}}, Header: header}
	}
	return &http.Request{Form: url.Values{"db": []string{db}, "q": []string{query}}, Header: header}
}

func CopyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Set(k, v)
		}
	}
}

func SetBasicAuth(req *http.Request, username string, password string, authSecure bool) {
	if authSecure {
		req.SetBasicAuth(util.AesDecrypt(username), util.AesDecrypt(password))
	} else {
		req.SetBasicAuth(username, password)
	}
}

func (backend *Backend) SetBasicAuth(req *http.Request) {
	SetBasicAuth(req, backend.Username, backend.Password, backend.AuthSecure)
}

func (backend *Backend) CheckActive() {
	for {
		backend.Active = backend.Ping()
		time.Sleep(time.Duration(backend.CheckInterval) * time.Second)
	}
}

func (backend *Backend) Ping() bool {
	resp, err := backend.Client.Get(backend.Url + "/ping")
	if err != nil {
		log.Print("http error: ", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		log.Printf("ping status code: %d, the backend is %s", resp.StatusCode, backend.Url)
		return false
	}
	return true
}

func (backend *Backend) Write(db string, p []byte) (err error) {
	var buf bytes.Buffer
	err = Compress(&buf, p)
	if err != nil {
		log.Print("compress error: ", err)
		return
	}
	return backend.WriteStream(db, &buf, true)
}

func (backend *Backend) WriteCompressed(db string, p []byte) error {
	buf := bytes.NewBuffer(p)
	return backend.WriteStream(db, buf, true)
}

func (backend *Backend) WriteStream(db string, stream io.Reader, compressed bool) error {
	q := url.Values{}
	q.Set("db", db)
	req, err := http.NewRequest("POST", backend.Url+"/write?"+q.Encode(), stream)
	if backend.Username != "" || backend.Password != "" {
		backend.SetBasicAuth(req)
	}
	if compressed {
		req.Header.Add("Content-Encoding", "gzip")
	}

	resp, err := backend.Client.Do(req)
	if err != nil {
		log.Print("http error: ", err)
		backend.Active = false
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return nil
	}
	log.Printf("write status code: %d, from: %s", resp.StatusCode, backend.Url)

	respbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print("readall error: ", err)
		return err
	}
	log.Printf("error response: %s", respbuf)

	switch resp.StatusCode {
	case 400:
		return ErrBadRequest
	case 401:
		return ErrUnauthorized
	case 404:
		return ErrNotFound
	case 500:
		return ErrInternal
	default: // mostly tcp connection timeout, or request entity too large
		return ErrUnknown
	}
	return nil
}

func (backend *Backend) Query(req *http.Request, w http.ResponseWriter, decompressed bool) ([]byte, error) {
	var err error
	if len(req.Form) == 0 {
		req.Form = url.Values{}
	}
	req.Form.Del("u")
	req.Form.Del("p")
	req.ContentLength = 0
	if backend.Username != "" || backend.Password != "" {
		backend.SetBasicAuth(req)
	}

	req.URL, err = url.Parse(backend.Url + "/query?" + req.Form.Encode())
	if err != nil {
		log.Print("internal url parse error: ", err)
		return nil, err
	}

	q := strings.TrimSpace(req.FormValue("q"))
	resp, err := backend.Transport.RoundTrip(req)
	if err != nil {
		log.Printf("query error: %s, the query is %s", err, q)
		return nil, err
	}
	defer resp.Body.Close()
	if w != nil {
		CopyHeader(w.Header(), resp.Header)
	}

	body := resp.Body
	if decompressed && resp.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(resp.Body)
		defer b.Close()
		if err != nil {
			log.Printf("unable to decode gzip body")
			return nil, err
		}
		body = b
	}

	return ioutil.ReadAll(body)
}

func (backend *Backend) QueryIQL(db, query string) ([]byte, error) {
	return backend.Query(NewRequest(db, query), nil, true)
}

func (backend *Backend) GetSeriesValues(db, query string) []string {
	var values []string
	p, err := backend.Query(NewRequest(db, query), nil, true)
	if err != nil {
		return values
	}
	series, _ := SeriesFromResponseBytes(p)
	for _, s := range series {
		for _, v := range s.Values {
			if s.Name == "databases" && v[0].(string) == "_internal" {
				continue
			}
			values = append(values, v[0].(string))
		}
	}
	return values
}

func (backend *Backend) GetDatabases() []string {
	return backend.GetSeriesValues("", "show databases")
}

func (backend *Backend) GetMeasurements(db string) []string {
	return backend.GetSeriesValues(db, "show measurements")
}

func (backend *Backend) GetTagKeys(db, meas string) []string {
	return backend.GetSeriesValues(db, fmt.Sprintf("show tag keys from \"%s\"", meas))
}

func (backend *Backend) GetFieldKeys(db, meas string) map[string][]string {
	fieldKeys := make(map[string][]string)
	query := fmt.Sprintf("show field keys from \"%s\"", meas)
	p, err := backend.Query(NewRequest(db, query), nil, true)
	if err != nil {
		return fieldKeys
	}
	series, _ := SeriesFromResponseBytes(p)
	for _, s := range series {
		for _, v := range s.Values {
			fk := v[0].(string)
			fieldKeys[fk] = append(fieldKeys[fk], v[1].(string))
		}
	}
	return fieldKeys
}

func (backend *Backend) DropMeasurement(db, meas string) ([]byte, error) {
	query := fmt.Sprintf("drop measurement \"%s\"", meas)
	return backend.Query(NewRequest(db, query), nil, true)
}
