package backend

import (
    "bytes"
    "compress/gzip"
    "crypto/tls"
    "encoding/binary"
    "errors"
    "fmt"
    "github.com/chengshiwen/influx-proxy/util"
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
    ErrBadRequest = errors.New("bad request")
    ErrNotFound   = errors.New("not found")
    ErrUnknown    = errors.New("unknown error")
)

type CBuffer struct {
    Buffer  *bytes.Buffer `json:"buffer"`
    Counter int           `json:"counter"`
}

type Backend struct {
    Name            string                      `json:"name"`
    Url             string                      `json:"url"`
    Username        string                      `json:"username"`
    Password        string                      `json:"password"`
    AuthSecure      bool                        `json:"auth_secure"`
    BufferMap       map[string]*CBuffer         `json:"buffer_map"`
    DataFlag        bool                        `json:"data_flag"`
    Producer        *os.File                    `json:"producer"`
    Consumer        *os.File                    `json:"consumer"`
    Meta            *os.File                    `json:"meta"`
    Client          *http.Client                `json:"client"`
    Transport       *http.Transport             `json:"transport"`
    Active          bool                        `json:"active"`
    LockDbMap       map[string]*sync.RWMutex    `json:"lock_db_map"`
    LockBuffer      *sync.RWMutex               `json:"lock_buffer"`
    LockFile        *sync.RWMutex               `json:"lock_file"`
}

// handle file

func (backend *Backend) OpenCacheFile(failDataDir string) {
    var err error
    fileNamePrefix := filepath.Join(failDataDir, backend.Name)
    backend.Producer, err = os.OpenFile(fileNamePrefix+".dat", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
    if err != nil {
        log.Printf("open producer error: %s %s", backend.Url, err)
        panic(err)
    }
    _, err = backend.Producer.Seek(0, io.SeekEnd)
    if err != nil {
        log.Printf("seek producer error: %s %s", backend.Url, err)
        panic(err)
    }

    backend.Consumer, err = os.OpenFile(fileNamePrefix+".dat", os.O_RDONLY, 0644)
    if err != nil {
        log.Printf("open consumer error: %s %s", backend.Url, err)
        panic(err)
    }

    backend.Meta, err = os.OpenFile(fileNamePrefix+".rec", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        log.Printf("open meta error: %s %s", backend.Url, err)
        panic(err)
    }

    backend.RollbackMeta()
}

func (backend *Backend) Write(p []byte) (err error) {
    backend.LockFile.Lock()
    defer backend.LockFile.Unlock()

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
    backend.LockFile.Lock()
    defer backend.LockFile.Unlock()
    return backend.DataFlag
}

func (backend *Backend) Read() (p []byte, err error) {
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
    backend.LockFile.Lock()
    defer backend.LockFile.Unlock()

    _, err = backend.Meta.Seek(0, io.SeekStart)
    if err != nil {
        log.Printf("seek meta error: %s %s", backend.Url, err)
        return
    }

    var offset int64
    err = binary.Read(backend.Meta, binary.BigEndian, &offset)
    if err != nil {
        log.Printf("read meta error: %s %s", backend.Url, err)
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
    backend.LockFile.Lock()
    defer backend.LockFile.Unlock()

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
}

// handle write buffer

func (backend *Backend) CheckBufferMapAndLockDbMap(db string) {
    if _, ok := backend.BufferMap[db]; !ok {
        backend.LockBuffer.Lock()
        backend.BufferMap[db] = &CBuffer{Buffer: &bytes.Buffer{}}
        backend.LockDbMap[db] = &sync.RWMutex{}
        defer backend.LockBuffer.Unlock()
    }
}

func (backend *Backend) WriteBuffer(data *LineData, bufferMaxSize int) (err error) {
    db := data.Db
    backend.CheckBufferMapAndLockDbMap(db)
    backend.LockDbMap[db].Lock()
    defer backend.LockDbMap[db].Unlock()

    backend.BufferMap[db].Buffer.Write(data.Line)
    backend.BufferMap[db].Counter++
    if backend.BufferMap[db].Counter > bufferMaxSize {
        err = backend.flushBuffer(db)
        if err != nil {
            log.Printf("flush buffer error: %s %s", db, err)
            return
        }
    }
    return
}

func (backend *Backend) flushBuffer(db string) (err error) {
    bc := backend.BufferMap[db]
    p := bc.Buffer.Bytes()
    bc.Buffer.Truncate(0)
    bc.Counter = 0
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
            log.Printf("unknown error %s", err)
        }
    }

    b := bytes.Join([][]byte{[]byte(db), p}, []byte(" "))
    err = backend.Write(b)
    if err != nil {
        log.Printf("write db and data to file error with db: %s, length: %d error: %s", db, len(p), err)
        return
    }
    return
}

// handle loop

func (backend *Backend) FlushBufferLoop(flushTimeout time.Duration) {
    for {
        select {
        case <- time.After(flushTimeout * time.Second):
            for db := range backend.BufferMap {
                if backend.BufferMap[db].Counter > 0 {
                    backend.LockDbMap[db].Lock()
                    err := backend.flushBuffer(db)
                    if err != nil {
                        log.Printf("flush buffer error: %s %s", db, err)
                    }
                    backend.LockDbMap[db].Unlock()
                }
            }
        }
    }
}

func (backend *Backend) RewriteLoop() {
    for {
        select {
        case <- time.After(util.RewriteInterval * time.Second):
            backend.Rewrite()
        }
    }
}

func (backend *Backend) Rewrite() {
    for backend.IsData() {
        if !backend.Active {
            time.Sleep(time.Second * util.WaitActiveInterval)
            continue
        }

        b, err := backend.Read()
        if err != nil {
            log.Print("rewrite read data error: ", err)
        }
        p := bytes.SplitN(b, []byte(" "), 2)

        err = backend.WriteCompressed(string(p[0]), p[1])
        if err != nil {
            log.Print("rewrite write compressed error: ", err)
        }
        backend.UpdateMeta()
    }
}

func (backend *Backend) CheckActive() {
    for {
        backend.Active = backend.Ping()
        time.Sleep(util.CheckPingInterval * time.Second)
    }
}

// handle http

func NewClient(url string) *http.Client {
    return &http.Client{Transport: NewTransport(url)}
}

func NewTransport(url string) *http.Transport {
    return &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: strings.HasPrefix(url, "https")}}
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
    if db == "" {
        return &http.Request{Form: url.Values{"q": []string{query}}, Header:make(map[string][]string)}
    }
    return &http.Request{Form: url.Values{"db": []string{db}, "q": []string{query}}, Header:make(map[string][]string)}
}

func CopyHeader(dst, src http.Header) {
    for k, vv := range src {
        for _, v := range vv {
            dst.Set(k, v)
        }
    }
}

func (backend *Backend) SetBasicAuth(req *http.Request) {
    if backend.AuthSecure {
        req.SetBasicAuth(util.AesDecrypt(backend.Username, util.CipherKey), util.AesDecrypt(backend.Password, util.CipherKey))
    } else {
        req.SetBasicAuth(backend.Username, backend.Password)
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

func (backend *Backend) WriteCompressed(db string, p []byte) error {
    buf := bytes.NewBuffer(p)
    return backend.WriteStream(db, buf, true)
}

func (backend *Backend) WriteStream(db string, stream io.Reader, compressed bool) error {
    q := url.Values{}
    q.Set("db", db)
    req, err := http.NewRequest(http.MethodPost, backend.Url+"/write?"+q.Encode(), stream)
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
    log.Print("write status code: ", resp.StatusCode)

    respbuf, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Print("readall error: ", err)
        return err
    }
    log.Printf("error response: %s", respbuf)

    switch resp.StatusCode {
    case 400:
        return ErrBadRequest
    case 404:
        return ErrNotFound
    default: // mostly tcp connection timeout
        return ErrUnknown
    }
    return nil
}

func (backend *Backend) Query(req *http.Request, w http.ResponseWriter, fromCluster bool) ([]byte, error) {
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

    respBody := resp.Body
    if fromCluster && resp.Header.Get("Content-Encoding") == "gzip" {
        respBody, err = gzip.NewReader(resp.Body)
        defer respBody.Close()
        if err != nil {
            log.Printf("unable to decode gzip body")
            return nil, err
        }
    }

    return ioutil.ReadAll(respBody)
}

func (backend *Backend) QueryIQL(db, query string) ([]byte, error) {
    return backend.Query(NewRequest(db, query), nil, false)
}

func (backend *Backend) GetSeriesValues(db, query string) []string {
    p, _ := backend.Query(NewRequest(db, query), nil, false)
    series, _ := SeriesFromResponseBytes(p)
    var values []string
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

func (backend *Backend) GetTagKeys(db, measure string) []string {
    return backend.GetSeriesValues(db, fmt.Sprintf("show tag keys from \"%s\"", measure))
}

func (backend *Backend) GetFieldKeys(db, measure string) map[string]string {
    query := fmt.Sprintf("show field keys from \"%s\"", measure)
    p, _ := backend.Query(NewRequest(db, query), nil, false)
    series, _ := SeriesFromResponseBytes(p)
    fieldKeys := make(map[string]string)
    for _, s := range series {
        for _, v := range s.Values {
            fieldKeys[v[0].(string)] = v[1].(string)
        }
    }
    return fieldKeys
}

func (backend *Backend) DropMeasurement(db, measure string) ([]byte, error) {
    query := fmt.Sprintf("drop measurement \"%s\"", measure)
    return backend.Query(NewRequest(db, query), nil, false)
}
