package consistent

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/chengshiwen/influx-proxy/util"
    "github.com/influxdata/influxdb1-client/models"
    "net/http"
    "net/url"
    "os"
    "regexp"
    "stathat.com/c/consistent"
    "strconv"
    "strings"
    "sync"
    "time"
)

// Proxy 集群
type Proxy struct {
    Circles                []*Circle                    `json:"circles"`     // 集群列表
    ListenAddr             string                       `json:"listen_addr"` // 服务监听地址
    DataDir                string                       `json:"data_dir"`    // 缓存文件目录
    DbList                 []string                     `json:"db_list"`     // 数据库列表
    VNodeSize              int                          `json:"vnode_size"`  // 虚拟节点数
    FlushSize              int                          `json:"flush_size"`  // 实例的缓冲区大小
    FlushTime              time.Duration                `json:"flush_time"`  // 实例的缓冲清空时间
    ForbiddenQuery         []*regexp.Regexp             `json:"forbidden_query"`
    ObligatedQuery         []*regexp.Regexp             `json:"obligated_query"`
    ClusteredQuery         []*regexp.Regexp             `json:"clustered_query"`
    BackendRebalanceStatus []map[string]*MigrationInfo  `json:"backend_re_balance_status"`
    BackendRecoveryStatus  []map[string]*MigrationInfo  `json:"backend_recovery_status"`
    BackendResyncStatus    []map[string]*MigrationInfo  `json:"backend_resync_status"`
    Username               string                       `json:"username"`
    Password               string                       `json:"password"`
}

// LineData 数据传输形式
type LineData struct {
    Line      []byte `json:"line"`      // 二进制数据
    Precision string `json:"precision"` // influxDb时间单位
    Db        string `json:"db"`
    Measure   string `json:"measure"`
}

// MigrationInfo 迁移信息
type MigrationInfo struct {
    CircleNum           int         `json:"circle_num"`
    Database            string      `json:"database"`
    Measurement         string      `json:"measurement"`
    BackendMeasureTotal int         `json:"backend_measure_total"`
    BMNeedMigrateCount  int         `json:"bm_need_migrate_count"`
    BMNotMigrateCount   int         `json:"bm_not_migrate_count"`
}

// NewCluster 新建集群
func NewProxy(file string) *Proxy {
    proxy := loadProxyJson(file)
    proxy.BackendRebalanceStatus = make([]map[string]*MigrationInfo, len(proxy.Circles))
    proxy.BackendRecoveryStatus = make([]map[string]*MigrationInfo, len(proxy.Circles))
    proxy.BackendResyncStatus = make([]map[string]*MigrationInfo, len(proxy.Circles))
    util.CheckPathAndCreate(proxy.DataDir)
    for circleNum, circle := range proxy.Circles {
        circle.CircleNum = circleNum
        proxy.initMigration(circle, circleNum)
        proxy.initCircle(circle)
    }
    proxy.ForbidQuery(util.ForbidCmds)
    proxy.EnsureQuery(util.SupportCmds)
    proxy.ClusterQuery(util.ClusterCmds)
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
    circle.Router = consistent.New()
    circle.Router.NumberOfReplicas = proxy.VNodeSize
    circle.UrlToBackend = make(map[string]*Backend)
    circle.BackendWgMap = make(map[string]*sync.WaitGroup)
    circle.WgMigrate = &sync.WaitGroup{}
    circle.ReadyMigrating = false
    circle.IsMigrating = false
    circle.StatusLock = &sync.RWMutex{}
    for _, backend := range circle.Backends {
        circle.BackendWgMap[backend.Url] = &sync.WaitGroup{}
        proxy.initBackend(circle, backend)
    }
}

func (proxy *Proxy) initBackend(circle *Circle, backend *Backend) {
    circle.Router.Add(backend.Url)
    circle.UrlToBackend[backend.Url] = backend

    backend.BufferMap = make(map[string]*BufferCounter)
    backend.LockDbMap = make(map[string]*sync.RWMutex)
    backend.LockFile = &sync.RWMutex{}
    backend.Client = &http.Client{}
    backend.Active = true
    backend.CreateCacheFile(proxy.DataDir)
    backend.Transport = new(http.Transport)
    if backend.MigrateCpuCores == 0 {
        backend.MigrateCpuCores = 1
    }

    for _, db := range proxy.DbList {
        backend.LockDbMap[db] = new(sync.RWMutex)
        backend.BufferMap[db] = &BufferCounter{Buffer: &bytes.Buffer{}}
    }
    go backend.CheckActive()
    go backend.CheckBufferAndSync(proxy.FlushTime)
    go backend.SyncFileData()
}

func (proxy *Proxy) initMigration(circle *Circle, circleNum int) {
    proxy.BackendRebalanceStatus[circleNum] = make(map[string]*MigrationInfo)
    proxy.BackendRecoveryStatus[circleNum] = make(map[string]*MigrationInfo)
    proxy.BackendResyncStatus[circleNum] = make(map[string]*MigrationInfo)
    for _, backend := range circle.Backends {
        proxy.BackendRebalanceStatus[circleNum][backend.Url] = &MigrationInfo{}
        proxy.BackendRecoveryStatus[circleNum][backend.Url] = &MigrationInfo{}
        proxy.BackendResyncStatus[circleNum][backend.Url] = &MigrationInfo{}
    }
}

// GetMachines 获取数据对应的三台备份物理主机
func (proxy *Proxy) GetMachines(key string) []*Backend {
    machines := make([]*Backend, 0)
    for circleNum, circle := range proxy.Circles {
        backendUrl, err := circle.Router.Get(key)
        if err != nil {
            util.Log.Errorf("circleNum:%v key:%+v err:%v", circleNum, key, err)
            continue
        }
        backend, ok := circle.UrlToBackend[backendUrl]
        if !ok {
            util.Log.Errorf("circleNum:%v UrlToBackend:%+v err:%+v", circleNum, circle.UrlToBackend, err)
            continue
        }
        machines = append(machines, backend)
    }
    return machines
}

// WriteData 写入数据操作
func (proxy *Proxy) WriteData(data *LineData) error {
    // 根据 Precision 对line做相应的调整
    data.Line = LineToNano(data.Line, data.Precision)

    // 得到对象将要存储的多个备份节点
    key := data.Db + "," + data.Measure
    backends := proxy.GetMachines(key)
    if len(backends) < 1 {
        util.Log.Errorf("request data:%v err:GetMachines length is 0", data)
        return errors.New("length is zero")
    }

    // 对象如果不是以\n结束的，则加上\n
    if !bytes.HasSuffix(data.Line, []byte("\n")) {
        data.Line = append(data.Line, []byte("\n")...)
    }

    // 顺序存储到多个备份节点上
    for _, backend := range backends {
        err := backend.WriteDataToBuffer(data, proxy.FlushSize)
        if err != nil {
            util.Log.Errorf("backend:%+v request data:%+v err:%+v", backend.Url, data, err)
            return err
        }
    }
    return nil
}

func (proxy *Proxy) AddBackend(circleNum int) ([]*Backend, error) {
    circle := proxy.Circles[circleNum]
    var res []*Backend
    for _, v := range circle.UrlToBackend {
        res = append(res, v)
    }
    return res, nil
}

func (proxy *Proxy) DeleteBackend(backendUrls []string) ([]*Backend, error) {
    var res []*Backend
    for _, v := range backendUrls {
        res = append(res, &Backend{Url: v, Client: &http.Client{}, Transport: new(http.Transport)})
    }
    return res, nil
}

func (proxy *Proxy) ForbidQuery(cmds []string) {
    for _, cmd := range cmds {
        r, err := regexp.Compile(cmd)
        if err != nil {
            panic(err)
            return
        }
        proxy.ForbiddenQuery = append(proxy.ForbiddenQuery, r)
    }
}

func (proxy *Proxy) EnsureQuery(cmds []string) {
    for _, cmd := range cmds {
        r, err := regexp.Compile(cmd)
        if err != nil {
            panic(err)
            return
        }
        proxy.ObligatedQuery = append(proxy.ObligatedQuery, r)
    }
}
func (proxy *Proxy) ClusterQuery(cmds []string) {
    for _, cmd := range cmds {
        r, err := regexp.Compile(cmd)
        if err != nil {
            panic(err)
            return
        }
        proxy.ClusteredQuery = append(proxy.ClusteredQuery, r)
    }
}

func (proxy *Proxy) CheckMeasurementQuery(q string) error {
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

func (proxy *Proxy) CheckClusterQuery(q string) error {
    if len(proxy.ClusteredQuery) != 0 {
        for _, pq := range proxy.ClusteredQuery {
            if pq.MatchString(q) {
                return nil
            }
        }
        return errors.New("query forbidden")
    }

    return nil
}

func (proxy *Proxy) Clear(dbs []string, circleNum int) error {
    circle := proxy.Circles[circleNum]
    for _, backend := range circle.Backends {
        circle.WgMigrate.Add(1)
        go proxy.clearCircle(circle, backend, dbs)
    }
    circle.WgMigrate.Wait()
    return nil
}

func (proxy *Proxy) clearCircle(circle *Circle, backend *Backend, dbs []string) {
    defer circle.WgMigrate.Done()

    for _, db := range dbs {
        // 单独出一个接口 删除接口
        // deleet old measure
        req := &http.Request{Form: url.Values{"q": []string{"show measurements"}, "db": []string{db}}}
        measures := circle.GetSeriesValues(req, []*Backend{backend})
        fmt.Printf("len-->%d db-->%+v\n", len(measures), db)
        for _, measure := range measures {
            key := db + "," + measure
            targetBackendUrl, err := circle.Router.Get(key)
            if err != nil {
                util.Log.Errorf("err:%+v")
                continue
            }

            if targetBackendUrl != backend.Url {
                fmt.Printf("src:%+v target:%+v \n", backend.Url, targetBackendUrl)
                delMeasureReq := &http.Request{
                    Form:   url.Values{"q": []string{fmt.Sprintf("drop measurement \"%s\" ", measure)}, "db": []string{db}},
                    Header: http.Header{"User-Agent": []string{"curl/7.54.0"}, "Accept": []string{"*/*"}},
                }
                _, e := backend.Query(delMeasureReq)
                if e != nil {
                    util.Log.Errorf("err:%+v", e)
                    continue
                }
            }
        }
    }
}

func (proxy *Proxy) Migrate(backend *Backend, dstBackends []*Backend, circle *Circle, db, measure string, lastSeconds int) {
    err := circle.Migrate(backend, dstBackends, db, measure, lastSeconds)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return
    }
    circle.BackendWgMap[backend.Url].Done()
}

func (proxy *Proxy) ClearMigrateStatus(data map[string]*MigrationInfo) {
    for _, backend := range data {
        backend.Database = ""
        backend.Measurement = ""
        backend.BackendMeasureTotal = 0
        backend.BMNeedMigrateCount = 0
        backend.BMNotMigrateCount = 0
    }
}

func (proxy *Proxy) Rebalance(backends []*Backend, circleNum int, databases []string) {
    // 标志正在迁移
    circle := proxy.Circles[circleNum]
    circle.SetIsMigrating(true)
    // databases判断
    if len(databases) == 0 {
        databases = proxy.DbList
    }

    // 异步迁移每台机器上的数据
    for _, backend := range backends {
        circle.WgMigrate.Add(1)
        go proxy.RebalanceBackend(backend, circleNum, databases)
    }
    circle.WgMigrate.Wait()
    circle.SetIsMigrating(false)
}

func (proxy *Proxy) RebalanceBackend(backend *Backend, circleNum int, databases []string) {
    var migrateCount int
    proxy.ClearMigrateStatus(proxy.BackendRebalanceStatus[circleNum])
    circle := proxy.Circles[circleNum]
    defer circle.WgMigrate.Done()

    // 设置进度 CircleNum
    proxy.BackendRebalanceStatus[circleNum][backend.Url].CircleNum = circleNum
    fmt.Printf("dbs-->%+v url-->%+v num-->%+v\n", databases, backend.Url, circleNum)
    for _, db := range databases {
        // 设置进度 db
        proxy.BackendRebalanceStatus[circleNum][backend.Url].Database = db
        // 获取db的measurement列表
        req := &http.Request{Form: url.Values{"q": []string{"show measurements"}, "db": []string{db}}}
        measures := circle.GetSeriesValues(req, []*Backend{backend})
        // 设置进度  BackendMeasureTotal
        proxy.BackendRebalanceStatus[circleNum][backend.Url].BackendMeasureTotal = len(measures)

        for key, measure := range measures {
            // 设置进度  Measurement
            proxy.BackendRebalanceStatus[circleNum][backend.Url].Measurement = measure

            // 获取目标实例
            dbMeasure := strings.Join([]string{db, ",", measure}, "")
            targetBackendUrl, err := circle.Router.Get(dbMeasure)
            if err != nil {
                util.Log.Errorf("dbMeasure:%+v err:%+v", dbMeasure, err)
            }
            // 判断data属否需要迁移
            if targetBackendUrl != backend.Url {
                // 设置进度 BMNeedMigrateCount计数器
                proxy.BackendRebalanceStatus[circleNum][backend.Url].BMNeedMigrateCount++
                util.RebalanceLog.Infof("srcUrl:%+v dstUrl:%+v key:%+v measure:%+v", backend.Url, targetBackendUrl, key, measure)
                dstBackend := circle.UrlToBackend[targetBackendUrl]
                migrateCount++
                circle.BackendWgMap[backend.Url].Add(1)
                go proxy.Migrate(backend, []*Backend{dstBackend}, circle, db, measure, 0)
                if migrateCount % backend.MigrateCpuCores == 0 {
                    circle.BackendWgMap[backend.Url].Wait()
                }
            } else {
                // 设置进度 BMNotMigrateCount 计数器
                proxy.BackendRebalanceStatus[circleNum][backend.Url].BMNotMigrateCount++
            }
        }
    }
}

func (proxy *Proxy) Recovery(fromCircleNum, toCircleNum int, recoveryBackendUrls []string, databases []string) {
    fromCircle := proxy.Circles[fromCircleNum]
    toCircle := proxy.Circles[toCircleNum]

    fromCircle.SetIsMigrating(true)
    toCircle.SetIsMigrating(true)
    defer fromCircle.SetIsMigrating(false)
    defer toCircle.SetIsMigrating(false)
    if len(databases) == 0 {
        databases = proxy.DbList
    }
    recoveryBackendUrlMap := make(map[string]bool)
    for _, v := range recoveryBackendUrls {
        recoveryBackendUrlMap[v] = true
    }
    for _, backend := range fromCircle.UrlToBackend {
        fromCircle.WgMigrate.Add(1)
        go proxy.RecoveryBackend(backend, fromCircle, toCircle, recoveryBackendUrlMap, databases)
    }
    fromCircle.WgMigrate.Wait()
}

func (proxy *Proxy) RecoveryBackend(backend *Backend, fromCircle, toCircle *Circle, recoveryBackendUrlMap map[string]bool, databases []string) {
    var migrateCount int
    defer fromCircle.WgMigrate.Done()
    fromCircleNum := fromCircle.CircleNum

    proxy.ClearMigrateStatus(proxy.BackendRecoveryStatus[fromCircleNum])
    proxy.BackendRecoveryStatus[fromCircleNum][backend.Url].CircleNum = fromCircleNum
    for _, db := range databases {
        proxy.BackendRecoveryStatus[fromCircleNum][backend.Url].Database = db
        req := &http.Request{Form: url.Values{"q": []string{"show measurements"}, "db": []string{db}}}
        measures := fromCircle.GetSeriesValues(req, []*Backend{backend})
        proxy.BackendRecoveryStatus[fromCircleNum][backend.Url].BackendMeasureTotal = len(measures)
        for k, measure := range measures {
            proxy.BackendRecoveryStatus[fromCircleNum][backend.Url].Measurement = measure
            dbMeasure := strings.Join([]string{db, ",", measure}, "")
            targetBackendUrl, err := toCircle.Router.Get(dbMeasure)
            if err != nil {
                util.Log.Errorf("err:%+v", err)
                continue
            }
            fmt.Printf("[==========]target:%+v db-->%+v measure-->%+v toCircle.BackendWgMap->%+v\n", targetBackendUrl, db, measure, toCircle.BackendWgMap)
            if _, ok := recoveryBackendUrlMap[targetBackendUrl]; ok {
                proxy.BackendRecoveryStatus[fromCircleNum][backend.Url].BMNeedMigrateCount++
                util.RecoveryLog.Infof("srcUrl:%+v dstUrl:%+v k:%+v measurement:%+v", backend.Url, targetBackendUrl, k, measure)
                dstBackend := toCircle.UrlToBackend[targetBackendUrl]
                migrateCount++
                fromCircle.BackendWgMap[backend.Url].Add(1)
                go proxy.Migrate(backend, []*Backend{dstBackend}, fromCircle, db, measure, 0)
                if migrateCount % backend.MigrateCpuCores == 0 {
                    fromCircle.BackendWgMap[backend.Url].Wait()
                }
            } else {
                proxy.BackendRecoveryStatus[fromCircleNum][backend.Url].BMNotMigrateCount++
            }
        }
    }
}

func (proxy *Proxy) Resync(databases []string, lastSeconds int) {
    if len(databases) == 0 {
        databases = proxy.DbList
    }
    for _, circle := range proxy.Circles {
        circle.SetIsMigrating(true)
        for _, backend := range circle.UrlToBackend {
            circle.WgMigrate.Add(1)
            go proxy.ResyncBackend(backend, circle, databases, lastSeconds)
        }
        circle.WgMigrate.Wait()
        circle.SetIsMigrating(false)
    }
}

func (proxy *Proxy) ResyncBackend(backend *Backend, circle *Circle, databases []string, lastSeconds int) {
    var migrateCount int
    defer circle.WgMigrate.Done()
    circleNum := circle.CircleNum

    proxy.ClearMigrateStatus(proxy.BackendResyncStatus[circleNum])
    proxy.BackendResyncStatus[circleNum][backend.Url].CircleNum = circleNum
    for _, db := range databases {
        proxy.BackendResyncStatus[circleNum][backend.Url].Database = db
        req := &http.Request{Form: url.Values{"q": []string{"show measurements"}, "db": []string{db}}}
        measures := circle.GetSeriesValues(req, []*Backend{backend})
        proxy.BackendResyncStatus[circleNum][backend.Url].BackendMeasureTotal = len(measures)
        for k, measure := range measures {
            proxy.BackendResyncStatus[circleNum][backend.Url].Measurement = measure
            dbMeasure := strings.Join([]string{db, ",", measure}, "")
            dstBackends := make([]*Backend, 0)
            for _, toCircle := range proxy.Circles {
                if toCircle.CircleNum != circleNum {
                    targetBackendUrl, err := toCircle.Router.Get(dbMeasure)
                    if err != nil {
                        util.Log.Errorf("err:%+v", err)
                        continue
                    }
                    dstBackend := toCircle.UrlToBackend[targetBackendUrl]
                    dstBackends = append(dstBackends, dstBackend)
                }
            }
            fmt.Printf("[==========]target:%+v db-->%+v measure-->%+v\n", dstBackends, db, measure)
            if len(dstBackends) > 0 {
                proxy.BackendResyncStatus[circleNum][backend.Url].BMNeedMigrateCount++
                util.ResyncLog.Infof("srcUrl:%+v dstUrl:%+v k:%+v measurement:%+v", backend.Url, dstBackends, k, measure)
                migrateCount++
                circle.BackendWgMap[backend.Url].Add(1)
                go proxy.Migrate(backend, dstBackends, circle, db, measure, lastSeconds)
                if migrateCount % backend.MigrateCpuCores == 0 {
                    circle.BackendWgMap[backend.Url].Wait()
                }
            } else {
                proxy.BackendResyncStatus[circleNum][backend.Url].BMNotMigrateCount++
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
