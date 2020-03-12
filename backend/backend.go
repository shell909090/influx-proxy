package backend

import (
    "bytes"
    "compress/gzip"
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

type BufferCounter struct {
    Buffer  *bytes.Buffer `json:"buffer"`
    Counter int           `json:"counter"`
}

// Backend 物理主机节点
type Backend struct {
    Name            string                      `json:"name"`              // backend 名称
    Url             string                      `json:"url"`               // backend 的数据库地址
    Username        string                      `json:"username"`          // backend 用户名
    Password        string                      `json:"password"`          // backend 密码
    BufferMap       map[string]*BufferCounter   `json:"buffer_map"`        // backend 写缓存
    Producer        *os.File                    `json:"producer"`          // backend 内容生产者
    Consumer        *os.File                    `json:"consumer"`          // backend 内容消费者
    Meta            *os.File                    `json:"meta"`              // backend 消费位置信息
    Client          *http.Client                `json:"client"`            // backend influxDb 客户端
    Transport       *http.Transport             `json:"transport"`         // backend influxDb 客户端
    Active          bool                        `json:"active"`            // backend http客户端状态
    LockDbMap       map[string]*sync.RWMutex    `json:"lock_db_map"`       // backend 锁
    LockFile        *sync.RWMutex               `json:"lock_file"`
}

func (backend *Backend) CreateCacheFile(failDataDir string) {
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

    err = backend.RollbackMeta()
}

func (backend *Backend) RollbackMeta() error {
    _, err := backend.Meta.Seek(0, io.SeekStart)
    if err != nil {
        log.Printf("seek meta error: %s %s", backend.Url, err)
        return err
    }

    var off int64
    err = binary.Read(backend.Meta, binary.BigEndian, &off)
    if err != nil {
        log.Printf("read meta error: %s %s", backend.Url, err)
        return err
    }

    _, err = backend.Consumer.Seek(off, io.SeekStart)
    if err != nil {
        log.Printf("seek consumer error: %s %s", backend.Url, err)
        return err
    }
    return nil
}

func (backend *Backend) UpdateMeta() error {
    producerOffset, err := backend.Producer.Seek(0, io.SeekCurrent)
    if err != nil {
        log.Printf("seek producer error: %s %s", backend.Url, err)
        return err
    }

    offset, err := backend.Consumer.Seek(0, io.SeekCurrent)
    if err != nil {
        log.Printf("seek consumer error: %s %s", backend.Url, err)
        return err
    }

    if producerOffset == offset {
        err = backend.CleanUp()
        if err != nil {
            log.Printf("cleanup error: %s %s", backend.Url, err)
            return err
        }
        offset = 0
    }

    _, err = backend.Meta.Seek(0, io.SeekStart)
    if err != nil {
        log.Printf("seek meta error: %s %s", backend.Url, err)
        return err
    }

    log.Printf("write meta: %s, %d", backend.Url, offset)
    err = binary.Write(backend.Meta, binary.BigEndian, &offset)
    if err != nil {
        log.Printf("write meta error: %s %s", backend.Url, err)
        return err
    }

    err = backend.Meta.Sync()
    if err != nil {
        log.Printf("sync meta error: %s %s", backend.Url, err)
        return err
    }

    return nil
}

func (backend *Backend) WriteToFile(db string, p []byte) error {
    backend.LockFile.Lock()
    defer backend.LockFile.Unlock()

    err := backend.WriteLengthAndData(p, true)
    if err != nil {
        log.Printf("db: %s, data length: %d, error: %s", db, len(p), err)
        return err
    }
    err = backend.WriteLengthAndData([]byte(db), false)
    if err != nil {
        log.Printf("db: %s, data length: %d, error: %s", db, len(p), err)
        return err
    }
    return nil
}

func (backend *Backend) WriteLengthAndData(p []byte, compress bool) error {
    var err error
    var buf bytes.Buffer

    if compress {
        err = Compress(&buf, p)
        if err != nil {
            log.Print("compress error: ", err)
            return err
        }
        p = buf.Bytes()
    }

    var length = uint32(len(p))
    err = binary.Write(backend.Producer, binary.BigEndian, length)
    if err != nil {
        log.Printf("data length: %d, error: %s", length, err)
        return err
    }
    n, err := backend.Producer.Write(p)
    if err != nil {
        log.Printf("data length: %d, error: %s", length, err)
        return err
    }
    if n != len(p) {
        log.Printf("data length: %d, success write: %d", length, n)
        return io.ErrShortWrite
    }

    err = backend.Producer.Sync()
    if err != nil {
        log.Print("sync error: ", err)
        return err
    }
    return nil
}

func (backend *Backend) hasData() bool {
    backend.LockFile.RLock()
    defer backend.LockFile.RUnlock()
    pro, _ := backend.Producer.Seek(0, io.SeekCurrent)
    con, _ := backend.Consumer.Seek(0, io.SeekCurrent)
    return pro - con > 0
}

func (backend *Backend) Read() ([]byte, error) {
    if !backend.hasData() {
        return nil, nil
    }
    var length uint32

    err := binary.Read(backend.Consumer, binary.BigEndian, &length)
    if err != nil {
        log.Print("read length error: ", err)
        return nil, err
    }
    p := make([]byte, length)

    _, err = io.ReadFull(backend.Consumer, p)
    if err != nil {
        log.Print("read error: ", err)
        return p, err
    }
    return p, nil
}

func (backend *Backend) CleanUp() (err error) {
    _, err = backend.Consumer.Seek(0, io.SeekStart)
    if err != nil {
        log.Print("seek consumer error: ", err)
        return err
    }
    err = backend.Producer.Truncate(0)
    if err != nil {
        log.Print("truncate error: ", err)
        return err
    }
    _, err = backend.Producer.Seek(0, io.SeekStart)
    if err != nil {
        log.Print("seek producer error: ", err)
        return err
    }
    return
}

func (backend *Backend) CheckBufferAndSync(syncDataTimeOut time.Duration) {
    for {
        select {
        case <- time.After(syncDataTimeOut * time.Second):
            for db := range backend.BufferMap {
                if backend.BufferMap[db].Counter > 0 {
                    backend.LockDbMap[db].Lock()
                    err := backend.checkBufferAndSync(db)
                    if err != nil {
                        log.Printf("check buffer and sync error: %s %s", db, err)
                    }
                    backend.LockDbMap[db].Unlock()
                }
            }
        }
    }
}

func (backend *Backend) SyncFileData() {
    for {
        select {
        case <- time.After(util.SyncFileInterval * time.Second):
            backend.syncFileDataToDb()
        }
    }
}

func (backend *Backend) checkBufferAndSync(db string) error {
    err := backend.WriteDataToDb(db)
    if err != nil {
        log.Printf("write data to db error: %s %s", db, err)
        return err
    }
    backend.BufferMap[db].Counter = 0
    backend.BufferMap[db].Buffer.Truncate(0)
    return err
}

func (backend *Backend) syncFileDataToDb() {
    for backend.hasData() {
        if !backend.Active {
            time.Sleep(time.Second * util.WaitActiveInterval)
            continue
        }

        p, err := backend.Read()
        if err != nil {
            log.Print("sync file data to db read error", err)
        }
        db, err := backend.Read()
        if err != nil {
            log.Print("sync file data to db read db error", err)
        }

        err = backend.Write(string(db), p, false)
        if err != nil {
            log.Print("sync file data to db write error", err)

        }
        backend.UpdateMeta()
    }
}

func (backend *Backend) WriteDataToBuffer(data *LineData, backendBufferMaxNum int) error {
    db := data.Db
    backend.LockDbMap[db].Lock()
    defer backend.LockDbMap[db].Unlock()
    backend.BufferMap[db].Buffer.Write(data.Line)
    backend.BufferMap[db].Counter++

    if backend.BufferMap[db].Counter > backendBufferMaxNum {
        err := backend.checkBufferAndSync(db)
        if err != nil {
            log.Printf("check buffer and sync error: %s %s", db, err)
            return err
        }
    }
    return nil
}

func (backend *Backend) WriteDataToDb(db string) error {
    if backend.BufferMap[db].Buffer.Len() == 0 {
        return errors.New("length is zero")
    }

    byteData := backend.BufferMap[db].Buffer.Bytes()
    if backend.Active {
        err := backend.Write(db, byteData, true)
        if err != nil {
            log.Printf("write data error: %s %s", db, err)
        }
        return err
    }

    err := backend.WriteToFile(db, byteData)
    if err != nil {
        log.Printf("write to file error: %s %s", db, err)
        return err
    }
    return nil
}

func (backend *Backend) CheckActive() {
    for {
        backend.Active = backend.Ping()
        time.Sleep(util.CheckPingInterval * time.Second)
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
        log.Printf("ping status code: %d, the backend is %s\n", resp.StatusCode, backend.Url)
        return false
    }
    return true
}

func (backend *Backend) Write(db string, p []byte, compressed bool) error {
    var err error
    var buf *bytes.Buffer
    if compressed {
        buf = &bytes.Buffer{}
        err = Compress(buf, p)
        if err != nil {
            log.Print("compress error: ", err)
            return err
        }
    } else {
        buf = bytes.NewBuffer(p)
    }

    err = backend.WriteStream(db, buf, compressed)
    return err
}

func (backend *Backend) WriteStream(db string, stream io.Reader, compressed bool) error {
    q := url.Values{}
    q.Set("db", db)
    req, err := http.NewRequest(http.MethodPost, backend.Url+"/write?"+q.Encode(), stream)
    req.SetBasicAuth(util.AesDecrypt(backend.Username, util.CipherKey), util.AesDecrypt(backend.Password, util.CipherKey))
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
    log.Printf("error response: %s\n", respbuf)

    switch resp.StatusCode {
    case 400:
        return errors.New("bad request")
    case 404:
        return errors.New("not found")
    default: // mostly tcp connection timeout
        return errors.New("unknown error")
    }
    return nil
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

func copyHeader(dst, src http.Header) {
    for k, vv := range src {
        for _, v := range vv {
            dst.Set(k, v)
        }
    }
}

func (backend *Backend) Query(req *http.Request, w http.ResponseWriter, fromCluster bool) ([]byte, error) {
    var err error
    if len(req.Form) == 0 {
        req.Form = url.Values{}
    }
    req.Form.Del("u")
    req.Form.Del("p")
    req.ContentLength = 0
    if backend.Username != "" && backend.Password != "" {
        req.SetBasicAuth(util.AesDecrypt(backend.Username, util.CipherKey), util.AesDecrypt(backend.Password, util.CipherKey))
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
        copyHeader(w.Header(), resp.Header)
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
    req := &http.Request{Form: url.Values{"db": []string{db}, "q": []string{query}}}
    return backend.Query(req, nil, false)
}

func (backend *Backend) GetSeriesValues(db, query string) []string {
    req := &http.Request{Form: url.Values{"db": []string{db}, "q": []string{query}}}
    p, _ := backend.Query(req, nil, false)
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

func (backend *Backend) GetMeasurements(db string) []string {
    return backend.GetSeriesValues(db, "show measurements")
}

func (backend *Backend) GetTagKeys(db, measure string) []string {
    return backend.GetSeriesValues(db, fmt.Sprintf("show tag keys from \"%s\"", measure))
}

func (backend *Backend) GetFieldKeys(db, measure string) map[string]string {
    query := fmt.Sprintf("show field keys from \"%s\"", measure)
    req := &http.Request{Form: url.Values{"db": []string{db}, "q": []string{query}}}
    p, _ := backend.Query(req, nil, false)
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
    req := &http.Request{Form: url.Values{"db": []string{db}, "q": []string{fmt.Sprintf("drop measurement \"%s\"", measure)}}}
    return backend.Query(req, nil, false)
}
