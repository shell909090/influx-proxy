package consist

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/chengshiwen/influx-proxy/mconst"
    "github.com/chengshiwen/influx-proxy/util"
    "github.com/influxdata/influxdb/models"
    "net/http"
    "net/url"
    "os"
    "regexp"
    "strconv"
    "sync"
    "time"
)

// Proxy 集群
type Proxy struct {
    Circles                []*Circle            `json:"circles"` // 集群列表
    ForbiddenQuery         []*regexp.Regexp     `json:"forbidden_query"`
    ObligatedQuery         []*regexp.Regexp     `json:"obligated_query"`
    ListenAddr             string               `json:"listen_addr"`   // 服务监听的端口
    FailDataDir            string               `json:"fail_data_dir"` // 写缓存文件目录
    DbList                 []string             `json:"db_list"`
    NumberOfReplicas       int                  `json:"number_of_replicas"`     // 虚拟节点数
    BackendBufferMaxNum    int                  `json:"backend_buffer_max_num"` // 实例的缓冲区大小
    SyncDataTimeOut        time.Duration        `json:"sync_data_time_out"`
    ProxyUsername          string               `json:"proxy_username"`
    ProxyPassword          string               `json:"proxy_password"`
}

// LineData 数据传输形式
type LineReq struct {
    Line      []byte `json:"line"`      // 二进制数据
    Precision string `json:"precision"` // influxDb时间单位
    Db        string `json:"db"`
    Measure   string `json:"measure"`
}

// NewCluster 新建集群
func NewProxy(file string) *Proxy {
    proxy := loadProxyJson(file)
    util.CheckPathAndCreate(proxy.FailDataDir)
    for circleNum, circle := range proxy.Circles {
        circle.CircleNum = circleNum
        proxy.initCircle(circle)
    }
    return proxy
}

// loadProxyJson 加载主机配置
func loadProxyJson(file string) *Proxy {
    proxy := &Proxy{}
    f, err := os.Open(file)
    defer f.Close()
    if err != nil {
        panic(err)
    }
    dec := json.NewDecoder(f)
    err = dec.Decode(proxy)
    return proxy
}

// initCircle 初始化哈希环
func (proxy *Proxy) initCircle(circle *Circle) {
    circle.Router = New()
    circle.Router.NumberOfReplicas = proxy.NumberOfReplicas
    circle.UrlToMap = make(map[string]*Backend)
    circle.BackendWgMap = make(map[string]*sync.WaitGroup)
    for _, backend := range circle.Backends {
        circle.BackendWgMap[backend.Url] = &sync.WaitGroup{}
        proxy.initBackend(circle, backend)
    }
}

func (proxy *Proxy) initBackend(circle *Circle, backend *Backend) {
    circle.Router.Add(backend.Url)
    circle.UrlToMap[backend.Url] = backend

    backend.BufferMap = make(map[string]*BufferCounter)
    backend.LockDbMap = make(map[string]*sync.RWMutex)
    backend.LockFile = &sync.RWMutex{}
    backend.Client = &http.Client{}
    backend.Active = true
    backend.CreateCacheFile(proxy.FailDataDir)
    backend.Transport = new(http.Transport)

    for _, db := range proxy.DbList {
        backend.LockDbMap[db] = new(sync.RWMutex)
        backend.BufferMap[db] = &BufferCounter{Buffer: &bytes.Buffer{}}
    }
    go backend.CheckActive()
    go backend.CheckBufferAndSync(proxy.SyncDataTimeOut)
    go backend.SyncFileData()
}

// GetMachines 获取数据对应的三台备份物理主机
func (proxy *Proxy) GetMachines(dbMeasure string) []*Backend {
    machines := make([]*Backend, 0)
    for circleNum, circle := range proxy.Circles {
        backendUrl, err := circle.Router.Get(dbMeasure)
        if err != nil {
            util.CustomLog.Errorf("circleNum:%v dbMeasure:%+v err:%v", circleNum, dbMeasure, err)
            continue
        }
        backend, ok := circle.UrlToMap[backendUrl]
        if !ok {
            util.CustomLog.Errorf("circleNum:%v UrlToMap:%+v err:%+v", circleNum, circle.UrlToMap, err)
            continue
        }
        machines = append(machines, backend)
    }
    return machines
}

// WriteData 写入数据操作
func (proxy *Proxy) WriteData(reqData *LineReq) error {
    // 根据 Precision 对line做相应的调整
    reqData.Line = LineToNano(reqData.Line, reqData.Precision)

    // 得到对象将要存储的多个备份节点
    dbMeasure := reqData.Db + "," + reqData.Measure
    backendS := proxy.GetMachines(dbMeasure)
    if len(backendS) < 1 {
        util.CustomLog.Errorf("reqData:%v err:GetMachines length is 0", reqData)
        return mconst.LengthNilErr
    }

    // 对象如果不是以\n结束的，则加上\n
    if reqData.Line[len(reqData.Line)-1] != '\n' {
        reqData.Line = bytes.Join([][]byte{reqData.Line, []byte("\n")}, []byte(""))
    }

    // 顺序存储到多个备份节点上
    for _, backend := range backendS {
        err := backend.WriteDataToBuffer(reqData, proxy.BackendBufferMaxNum)
        if err != nil {
            util.CustomLog.Errorf("backend:%+v reqData:%+v err:%+v", backend.Url, reqData, err)
            return err
        }
    }
    return nil
}

func (proxy *Proxy) AddBackend(circleNum int) ([]*Backend, error) {
    circle := proxy.Circles[circleNum]
    var res []*Backend
    for _, v := range circle.UrlToMap {
        res = append(res, v)
    }
    return res, nil
}

func (proxy *Proxy) DeleteBackend(backendUrls []string, circleNum int) ([]*Backend, error) {
    var res []*Backend
    for _, v := range backendUrls {
        res = append(res, &Backend{Url: v, Client: &http.Client{}, Transport: new(http.Transport)})
    }
    return res, nil
}

func GetMeasurementList(circle *Circle, req *http.Request, backends []*Backend) []string {
    p, _ := circle.QueryShow(req, backends)
    res, _ := GetSeriesArray(p)
    var databases []string
    for _, v := range res {
        for _, vv := range v.Values {
            if vv[0] == "_internal" {
                continue
            }
            databases = append(databases, vv[0].(string))
        }
    }
    return databases
}

func (proxy *Proxy) ForbidQuery(s string) (err error) {
    r, err := regexp.Compile(s)
    if err != nil {
        return
    }

    proxy.ForbiddenQuery = append(proxy.ForbiddenQuery, r)
    return
}

func (proxy *Proxy) EnsureQuery(s string) (err error) {
    r, err := regexp.Compile(s)
    if err != nil {
        return
    }

    proxy.ObligatedQuery = append(proxy.ObligatedQuery, r)
    return
}

func (proxy *Proxy) CheckQuery(q string) error {
    if len(proxy.ForbiddenQuery) != 0 {
        for _, fq := range proxy.ForbiddenQuery {
            if fq.MatchString(q) {
                return errors.New("query forbidden")
            }
        }
    }

    if len(proxy.ObligatedQuery) != 0 {
        for _, pq := range proxy.ObligatedQuery {
            if pq.MatchString(q) {
                return nil
            }
        }
        return errors.New("query forbidden")
    }

    return nil
}

func (proxy *Proxy) ClearMeasure(dbs []string, circleNum int) error {
    circle := proxy.Circles[circleNum]
    for _, backend := range circle.Backends {
        go proxy.clearMeasure(circle, dbs, backend)
    }
    return nil
}

func (proxy *Proxy) clearMeasure(circle *Circle, dbs []string, backend *Backend) {
    for _, db := range dbs {
        // 单独出一个接口 删除接口
        // deleet old measure
        req := &http.Request{Form: url.Values{"q": []string{"show measurements"}, "db": []string{db}}}
        measures := GetMeasurementList(circle, req, []*Backend{backend})
        fmt.Printf("len-->%d db-->%+v\n", len(measures), db)
        for _, measure := range measures {
            dbMeasure := db + "," + measure
            targetBackendUrl, err := circle.Router.Get(dbMeasure)
            if err != nil {
                util.CustomLog.Errorf("err:%+v")
                continue
            }

            if targetBackendUrl != backend.Url {
                fmt.Printf("src:%+v target:%+v \n", backend.Url, targetBackendUrl)
                delMeasureReq := &http.Request{
                    Form:   url.Values{"q": []string{fmt.Sprintf("drop measurement \"%s\" ", measure)}, "db": []string{db}},
                    Header: http.Header{"User-Agent": []string{"curl/7.54.0"}, "Accept": []string{"*/*"}},
                }
                _, e := backend.QueryShow(delMeasureReq)
                if e != nil {
                    util.CustomLog.Errorf("err:%+v", e)
                    continue
                }
            }
        }
    }
}

func Int64ToBytes(n int64) []byte {
    return []byte(strconv.FormatInt(n, 10))
}

func BytesToInt64(buf []byte) int64 {
    var res int64 = 0
    var length = len(buf)
    for i := 0; i < length; i++ {
        res = res * 10 + int64(buf[i]-'0')
    }
    return res
}

func ScanTime(buf []byte) (bool, int) {
    i := len(buf) - 1
    for ; i >= 0; i-- {
        if buf[i] < '0' || buf[i] > '9' {
            break
        }
    }
    return i > 0 && i < len(buf) - 1 && (buf[i] == ' ' || buf[i] == '\t' || buf[i] == 0), i
}

func LineToNano(line []byte, precision string) []byte {
    line = bytes.TrimRight(line, " \t\r\n")
    if precision != "ns" {
        if found, pos := ScanTime(line); found {
            if precision == "u" {
                return append(line, []byte("000")...)
            } else if precision == "ms" {
                return append(line, []byte("000000")...)
            } else if precision == "s" {
                return append(line, []byte("000000000")...)
            } else {
                mul := models.GetPrecisionMultiplier(precision)
                nano := BytesToInt64(line[pos+1:]) * mul
                bytenano := Int64ToBytes(nano)
                return bytes.Join([][]byte{line[:pos], bytenano}, []byte(" "))
            }
        }
    }
    return line
}
