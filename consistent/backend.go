package consistent

import (
    "bytes"
    "compress/gzip"
    "encoding/binary"
    "fmt"
    "github.com/chengshiwen/influx-proxy/util"
    "io"
    "io/ioutil"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
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
    FileProducer    *os.File                    `json:"file_producer"`     // backend 文件内容生产者
    FileConsumer    *os.File                    `json:"file_consumer"`     // backend 文件内容消费者
    FileConsumerPos *os.File                    `json:"file_consumer_pos"` // backend 消费者位置信息
    Client          *http.Client                `json:"client"`            // backend influxDb 客户端
    Active          bool                        `json:"active"`            // backend http客户端状态
    SyncFailedData  bool                        `json:"sync_failed_data"`  // backend 是否正在同步失败时
    LockDbMap       map[string]*sync.RWMutex    `json:"lock_db_map"`       // backend 锁
    LockFile        *sync.RWMutex               `json:"lock_file"`
    Transport       *http.Transport             `json:"transport"`
}

// CreateCacheFile 创建缓存文件
func (backend *Backend) CreateCacheFile(failDataDir string) {
    var err error
    fileNamePrefix := filepath.Join(failDataDir, backend.Name)
    backend.FileProducer, err = os.OpenFile(fileNamePrefix+".dat", os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
    if err != nil {
        util.Log.Errorf("backend_url:%+v err:%+v", backend.Url, err)
        panic(err)
    }
    _, err = backend.FileProducer.Seek(0, io.SeekEnd)
    if err != nil {
        util.Log.Errorf("backend_url:%+v err:%+v", backend.Url, err)
        panic(err)
    }

    backend.FileConsumer, err = os.OpenFile(fileNamePrefix+".dat", os.O_RDONLY, 0644)
    if err != nil {
        util.Log.Errorf("backend_url:%+v err:%+v", backend.Url, err)
        panic(err)
    }

    backend.FileConsumerPos, err = os.OpenFile(fileNamePrefix+".rec", os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        util.Log.Errorf("backend_url:%+v err:%+v", backend.Url, err)
        panic(err)
    }

    // read consumer pos from .rec
    err = backend.ReadConsumerPos()
}

func (backend *Backend) ReadConsumerPos() error {
    _, err := backend.FileConsumerPos.Seek(0, io.SeekStart)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    var off int64
    err = binary.Read(backend.FileConsumerPos, binary.BigEndian, &off)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    _, err = backend.FileConsumer.Seek(off, io.SeekStart)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }
    return nil
}

func (backend *Backend) UpdateConsumerPos() error {
    producerOffset, err := backend.FileProducer.Seek(0, io.SeekCurrent)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    consumerOffset, err := backend.FileConsumer.Seek(0, io.SeekCurrent)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    if producerOffset == consumerOffset {
        err = backend.CleanUp()
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            return err
        }
        consumerOffset = 0
    }

    _, err = backend.FileConsumerPos.Seek(0, io.SeekStart)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    err = binary.Write(backend.FileConsumerPos, binary.BigEndian, &consumerOffset)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    err = backend.FileConsumerPos.Sync()
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    return nil
}

// Write 写到文件中
func (backend *Backend) WriteToFile(db string, p []byte) error {
    backend.LockFile.Lock()
    defer backend.LockFile.Unlock()

    err := backend.WriteLengthAndData(p, true)
    if err != nil {
        util.Log.Errorf("db:%+v data_length:%+v err:%+v", db, len(p), err)
        return err
    }
    err = backend.WriteLengthAndData([]byte(db), false)
    if err != nil {
        util.Log.Errorf("db:%+v data_length:%+v err:%+v", db, len(p), err)
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
            util.Log.Errorf("Compress_err:%+v", err)
            return err
        }
        p = buf.Bytes()
    }

    var length = uint32(len(p))
    err = binary.Write(backend.FileProducer, binary.BigEndian, length)
    if err != nil {
        util.Log.Errorf("data_length:%+v err:%+v", length, err)
        return err
    }
    n, err := backend.FileProducer.Write(p)
    if err != nil {
        util.Log.Errorf("data_length:%+v err:%+v", length, err)
        return err
    }
    if n != len(p) {
        util.Log.Errorf("data_length:%+v success_write:%+v", length, n)
        return io.ErrShortWrite
    }

    err = backend.FileProducer.Sync()
    if err != nil {
        util.Log.Errorf("Sync err:%+V", err)
        return err
    }
    return nil
}

func (backend *Backend) IsData() bool {
    backend.LockFile.RLock()
    defer backend.LockFile.RUnlock()

    pro, _ := backend.FileProducer.Seek(0, io.SeekCurrent)
    con, _ := backend.FileConsumer.Seek(0, io.SeekCurrent)
    if pro-con > 0 {
        return true
    }
    return false
}

func (backend *Backend) Read() ([]byte, error) {
    if !backend.IsData() {
        return nil, nil
    }
    var length uint32

    err := binary.Read(backend.FileConsumer, binary.BigEndian, &length)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return nil, err
    }
    p := make([]byte, length)

    _, err = io.ReadFull(backend.FileConsumer, p)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return p, err
    }
    return p, nil
}

// CleanUp 清空文件
func (backend *Backend) CleanUp() (err error) {
    _, err = backend.FileConsumer.Seek(0, io.SeekStart)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }

    err = backend.FileProducer.Truncate(0)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }
    _, err = backend.FileProducer.Seek(0, io.SeekStart)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }
    return
}

// ListenLineChan 物理主机的守护进程用于写入数据
func (backend *Backend) CheckBufferAndSync(syncDataTimeOut time.Duration) {
    for {
        select {
        case <- time.After(syncDataTimeOut * time.Second):
            for db := range backend.BufferMap {
                if backend.BufferMap[db].Counter > 0 {
                    backend.LockDbMap[db].Lock()
                    err := backend.checkBufferAndSync(db)
                    if err != nil {
                        util.Log.Errorf("checkBufferAndSync db:%+v err:%+v", db, err)
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
        case <- time.After(util.SyncFileData * time.Second):
            backend.syncFileDataToDb()
        }
    }
}

// 达到数量或者超时清空buffer到数据库
func (backend *Backend) checkBufferAndSync(db string) error {
    util.Log.Debugf("url:%+v db:%+v Counter%+v", backend.Url, db, backend.BufferMap[db].Counter)
    err := backend.WriteDataToDb(db)
    if err != nil {
        util.Log.Errorf("db:%+v err:%+v", db, err)
        return err
    }

    // 清空buffer和计数器
    backend.BufferMap[db].Counter = 0
    backend.BufferMap[db].Buffer.Truncate(0)
    return err
}

func (backend *Backend) syncFileDataToDb() {
    for backend.IsData() {
        if !backend.Active {
            time.Sleep(time.Second * util.AwaitActiveTimeOut)
            continue
        }

        // ?????
        p, err := backend.Read()
        if err != nil {
            util.Log.Errorf("err:%+v", err)
        }
        db, err := backend.Read()
        if err != nil {
            util.Log.Errorf("err:%+v", err)
        }

        err = backend.Write(string(db), p, false)
        if err != nil {
            util.Log.Errorf("err:%+v", err)

        }
        backend.UpdateConsumerPos()
    }
}

// WriteDataToDb 写入对应backend的buffer中
func (backend *Backend) WriteDataToBuffer(data *LineData, backendBufferMaxNum int) error {
    db := data.Db
    //获取当前实例中对应db的锁
    backend.LockDbMap[db].Lock()
    defer backend.LockDbMap[db].Unlock()
    //写数据并判断阈值
    backend.BufferMap[db].Buffer.Write(data.Line)
    backend.BufferMap[db].Counter++

    if backend.BufferMap[db].Counter > backendBufferMaxNum {
        err := backend.checkBufferAndSync(db)
        if err != nil {
            util.Log.Errorf("db:%+v err:%+v", db, err)
            return err
        }
    }
    return nil
}

// WriteDataToDb 达到某种条件后写入到db中，失败写文件
func (backend *Backend) WriteDataToDb(db string) error {
    // 没有数据返回nil error
    if backend.BufferMap[db].Buffer.Len() == 0 {
        return util.LengthNilErr
    }

    // 对应实例的机器如果存活，则写入数据库
    byteData := backend.BufferMap[db].Buffer.Bytes()
    if backend.Active {
        err := backend.Write(db, byteData, true)
        if err != nil {
            util.Log.Errorf("db:%+v err:%+v", db, err)
            return err
        }
    }

    // 写入数据库失败，则写到文件中，等待同步
    err := backend.WriteToFile(db, byteData)
    if err != nil {
        util.Log.Errorf("db:%+v err:%+v", db, err)
        return err
    }
    return nil
}

// CheckActive 验活
func (backend *Backend) CheckActive() {
    for {
        backend.Active = backend.Ping()
        time.Sleep(util.CheckPingTimeOut * time.Second)
    }
}

// Ping ...
func (backend *Backend) Ping() bool {
    resp, err := backend.Client.Get(backend.Url + "/ping")
    if err != nil {
        return false
    }
    defer resp.Body.Close()
    if resp.StatusCode != 204 {
        return false
    }
    return true
}

// WriteCompress 压缩在写入db
func (backend *Backend) Write(db string, p []byte, compress bool) error {
    var err error
    var buf *bytes.Buffer
    // 压缩对象
    if compress {
        buf = &bytes.Buffer{}
        err = Compress(buf, p)
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            return err
        }
    } else {
        buf = bytes.NewBuffer(p)
    }

    // 发送http请求，写入数据库
    err = backend.WriteStream(db, buf)
    return err
}

// WriteStream 流式写入db
func (backend *Backend) WriteStream(db string, stream io.Reader) error {
    q := url.Values{}
    q.Set("db", db)
    req, err := http.NewRequest("POST", backend.Url+"/write?"+q.Encode(), stream)
    req.Header.Add("Content-Encoding", "gzip")
    req.SetBasicAuth(util.AesDecrypt(backend.Username, util.CIPHER_KEY), util.AesDecrypt(backend.Password, util.CIPHER_KEY))
    resp, err := backend.Client.Do(req)
    if err != nil {
        fmt.Printf("backend.Client.Do err:%+v\n", err)
        backend.Active = false
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode == 204 {
        return nil
    }
    _, err = ioutil.ReadAll(resp.Body)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return err
    }
    return nil
}

// Compress 压缩存储数据
func Compress(buf *bytes.Buffer, p []byte) (err error) {
    zip := gzip.NewWriter(buf)
    defer zip.Close()
    n, err := zip.Write(p)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return
    }
    if n != len(p) {
        err = io.ErrShortWrite
        util.Log.Errorf("err:%+v", err)
        return
    }
    return
}

// 发送http查询数据
func (backend *Backend) Query(req *http.Request) ([]byte, error) {
    var err error
    req.Form.Del("u")
    req.Form.Del("p")
    req, err = http.NewRequest("POST", backend.Url+"/query?"+req.Form.Encode(), nil)
    req.SetBasicAuth(util.AesDecrypt(backend.Username, util.CIPHER_KEY), util.AesDecrypt(backend.Password, util.CIPHER_KEY))
    resp, err := backend.Client.Do(req)
    defer resp.Body.Close()
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return nil, err
    }
    res, err := ioutil.ReadAll(resp.Body)
    return res, err
}
