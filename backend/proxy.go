package backend

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/chengshiwen/influx-proxy/util"
    "io/ioutil"
    "log"
    "net/http"
    "os"
    "regexp"
    "stathat.com/c/consistent"
    "strings"
    "sync"
    "sync/atomic"
    "time"
)

type Proxy struct {
    Circles                 []*Circle                       `json:"circles"`
    ListenAddr              string                          `json:"listen_addr"`
    DbList                  []string                        `json:"db_list"`
    DbMap                   map[string]bool                 `json:"db_map"`
    DataDir                 string                          `json:"data_dir"`
    MlogDir                 string                          `json:"mlog_dir"`
    VNodeSize               int                             `json:"vnode_size"`
    FlushSize               int                             `json:"flush_size"`
    FlushTime               time.Duration                   `json:"flush_time"`
    Username                string                          `json:"username"`
    Password                string                          `json:"password"`
    AuthSecure              bool                            `json:"auth_secure"`
    HTTPSEnabled            bool                            `json:"https_enabled"`
    HTTPSCert               string                          `json:"https_cert"`
    HTTPSKey                string                          `json:"https_key"`
    ForbiddenQuery          []*regexp.Regexp                `json:"forbidden_query"`
    ObligatedQuery          []*regexp.Regexp                `json:"obligated_query"`
    ClusteredQuery          []*regexp.Regexp                `json:"clustered_query"`
    IsResyncing             bool                            `json:"is_resyncing"`
    MigrateCpus             int                             `json:"migrate_cpus"`
    MigrateStats            []map[string]*MigrateInfo       `json:"migrate_stats"`
    Lock                    *sync.RWMutex                   `json:"lock"`
}

type LineData struct {
    Db        string `json:"db"`
    Line      []byte `json:"line"`
    Precision string `json:"precision"`
}

type MigrateInfo struct {
    DatabaseTotal    int32 `json:"database_total"`
    DatabaseDone     int32 `json:"database_done"`
    MeasurementTotal int32 `json:"measurement_total"`
    MeasurementDone  int32 `json:"measurement_done"`
    MigrateCount     int32 `json:"migrate_count"`
    InPlaceCount     int32 `json:"inplace_count"`
}

func NewProxy(file string) (proxy *Proxy, err error) {
    proxy, err = LoadProxyConfig(file)
    if err != nil {
        return
    }
    err = util.MakeDir(proxy.DataDir)
    if err != nil {
        return
    }
    proxy.MigrateStats = make([]map[string]*MigrateInfo, len(proxy.Circles))
    proxy.Lock = &sync.RWMutex{}
    for circleId, circle := range proxy.Circles {
        circle.CircleId = circleId
        proxy.initCircle(circle)
        proxy.initMigrateStats(circle)
    }
    proxy.DbMap = make(map[string]bool)
    for _, db := range proxy.DbList {
        proxy.DbMap[db] = true
    }
    proxy.ForbidQuery(ForbidCmds)
    proxy.EnsureQuery(SupportCmds)
    proxy.ClusterQuery(ClusterCmds)
    return
}

func LoadProxyConfig(file string) (proxy *Proxy, err error) {
    proxy = &Proxy{}
    f, err := os.Open(file)
    defer f.Close()
    if err != nil {
        return
    }
    dec := json.NewDecoder(f)
    err = dec.Decode(proxy)
    if err != nil {
        return
    }
    if proxy.DataDir == "" {
        proxy.DataDir = "data"
    }
    if proxy.MlogDir == "" {
        proxy.MlogDir = "log"
    }
    if proxy.MigrateCpus <= 0 {
        proxy.MigrateCpus = 1
    }
    log.Printf("%d circles loaded from file", len(proxy.Circles))
    for id, circle := range proxy.Circles {
        log.Printf("circle %d: %d backends loaded", id, len(circle.Backends))
    }
    return
}

func (proxy *Proxy) initCircle(circle *Circle) {
    circle.Router = consistent.New()
    circle.Router.NumberOfReplicas = proxy.VNodeSize
    circle.UrlToBackend = make(map[string]*Backend)
    circle.BackendWgMap = make(map[string]*sync.WaitGroup)
    circle.IsMigrating = false
    circle.MigrateWg = &sync.WaitGroup{}
    circle.Lock = &sync.RWMutex{}
    for _, backend := range circle.Backends {
        circle.BackendWgMap[backend.Url] = &sync.WaitGroup{}
        proxy.initBackend(circle, backend)
    }
}

func (proxy *Proxy) initBackend(circle *Circle, backend *Backend) {
    circle.Router.Add(backend.Url)
    circle.UrlToBackend[backend.Url] = backend

    backend.AuthSecure = proxy.AuthSecure
    backend.BufferMap = make(map[string]*CBuffer)
    backend.Client = NewClient(backend.Url)
    backend.Transport = NewTransport(backend.Url)
    backend.Active = true
    backend.LockDbMap = make(map[string]*sync.RWMutex)
    backend.LockBuffer = &sync.RWMutex{}
    backend.LockFile = &sync.RWMutex{}
    backend.OpenFile(proxy.DataDir)

    go backend.CheckActive()
    go backend.FlushBufferLoop(proxy.FlushTime)
    go backend.RewriteLoop()
}

func (proxy *Proxy) initMigrateStats(circle *Circle) {
    circleId := circle.CircleId
    proxy.MigrateStats[circleId] = make(map[string]*MigrateInfo)
    for _, backend := range circle.Backends {
        proxy.MigrateStats[circleId][backend.Url] = &MigrateInfo{}
    }
}

func GetKey(db, meas string) string {
    return fmt.Sprintf("%s,%s", db, meas)
}

func (proxy *Proxy) GetBackends(key string) []*Backend {
    backends := make([]*Backend, 0)
    for circleId, circle := range proxy.Circles {
        backendUrl, err := circle.Router.Get(key)
        if err != nil {
            log.Printf("circleId: %d, key: %s, error: %s", circleId, key, err)
            continue
        }
        backend := circle.UrlToBackend[backendUrl]
        backends = append(backends, backend)
    }
    return backends
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

func (proxy *Proxy) CheckMeasurementQuery(q string) bool {
    if len(proxy.ForbiddenQuery) != 0 {
        for _, fq := range proxy.ForbiddenQuery {
            if fq.MatchString(q) {
                return false
            }
        }
    }
    if len(proxy.ObligatedQuery) != 0 {
        for _, pq := range proxy.ObligatedQuery {
            if pq.MatchString(q) {
                return true
            }
        }
        return false
    }
    return true
}

func (proxy *Proxy) CheckClusterQuery(q string) bool {
    if len(proxy.ClusteredQuery) != 0 {
        for _, pq := range proxy.ClusteredQuery {
            if pq.MatchString(q) {
                return true
            }
        }
        return false
    }
    return true
}

func (proxy *Proxy) CheckCreateDatabaseQuery(q string) (string, bool) {
    if len(proxy.ClusteredQuery) > 1 {
        matches := proxy.ClusteredQuery[len(proxy.ClusteredQuery)-2].FindStringSubmatch(q)
        if len(matches) == 2 {
            return matches[1], true
        }
    }
    return "", false
}

func (proxy *Proxy) CheckDeleteOrDropQuery(q string) bool {
    if len(proxy.ClusteredQuery) > 0 {
        return proxy.ClusteredQuery[len(proxy.ClusteredQuery)-1].MatchString(q)
    }
    return false
}

func (proxy *Proxy) CreateDatabase(w http.ResponseWriter, req *http.Request) ([]byte, error) {
    var body []byte
    var err error
    for _, circle := range proxy.Circles {
        if !circle.CheckStatus() {
            return nil, errors.New(fmt.Sprintf("circle %d not health", circle.CircleId))
        }
    }
    for _, circle := range proxy.Circles {
        body, err = circle.QueryCluster(w, req)
        if err != nil {
            return nil, err
        }
    }
    return body, nil
}

func (proxy *Proxy) DeleteOrDropMeasurement(w http.ResponseWriter, req *http.Request) ([]byte, error) {
    var body []byte
    var err error
    db := req.FormValue("db")
    q := strings.TrimSpace(req.FormValue("q"))
    meas, err := GetMeasurementFromInfluxQL(q)
    if err != nil {
        return nil, err
    }
    var reqBodyBytes []byte
    if req.Body != nil {
        reqBodyBytes, _ = ioutil.ReadAll(req.Body)
    }
    key := GetKey(db, meas)
    backends := proxy.GetBackends(key)
    for _, backend := range backends {
        req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
        body, err = backend.Query(req, w, false)
        if err != nil {
            return nil, err
        }
    }
    return body, nil
}

func (proxy *Proxy) WriteData(data *LineData) {
    data.Line = LineToNano(data.Line, data.Precision)

    meas, err := ScanKey(data.Line)
    if err != nil {
        log.Printf("scan key error: %s", err)
        return
    }
    key := GetKey(data.Db, meas)
    backends := proxy.GetBackends(key)
    // fmt.Printf("%s key: %s; backends:", time.Now().Format("2006-01-02 15:04:05"), key)
    // for _, b := range backends {
    //     fmt.Printf(" %s", b.Name)
    // }
    // fmt.Printf("\n")
    if len(backends) == 0 {
        log.Printf("write data: %v, error: get backends length is 0", data)
        return
    }

    if !bytes.HasSuffix(data.Line, []byte("\n")) {
        data.Line = append(data.Line, []byte("\n")...)
    }

    for _, backend := range backends {
        err := backend.WriteBuffer(data, proxy.FlushSize)
        if err != nil {
            log.Print("write data to buffer: ", backend.Url, data, err)
            return
        }
    }
    return
}

func (proxy *Proxy) GetDatabases() []string {
   for _, circle := range proxy.Circles {
       for _, backend := range circle.Backends {
           if backend.Active {
               return backend.GetDatabases()
           }
       }
   }
   return nil
}

func (proxy *Proxy) GetBackendUrls(backends []*Backend) []string {
    backendUrls := make([]string, len(backends))
    for k, b := range backends {
        backendUrls[k] = b.Url
    }
    return backendUrls
}

func (proxy *Proxy) Migrate(backend *Backend, dstBackends []*Backend, circle *Circle, db, meas string, seconds int) {
    err := circle.Migrate(backend, dstBackends, db, meas, seconds)
    if err != nil {
        util.Mlog.Printf("migrate error:%s src:%s dst:%v circle:%d db:%s measurement:%s seconds:%d", err, backend.Url, proxy.GetBackendUrls(dstBackends), circle.CircleId, db, meas, seconds)
    }
    defer circle.BackendWgMap[backend.Url].Done()
}

func (proxy *Proxy) Rebalance(circleId int, backends []*Backend, databases []string) {
    util.SetMLog(proxy.MlogDir, "rebalance.log")
    util.Mlog.Printf("rebalance start")
    circle := proxy.Circles[circleId]
    circle.SetMigrating(true)
    if len(databases) == 0 {
        databases = proxy.GetDatabases()
    }
    proxy.ClearMigrateStats()
    for _, backend := range backends {
        circle.MigrateWg.Add(1)
        go proxy.RebalanceBackend(backend, circle, databases)
    }
    circle.MigrateWg.Wait()
    defer circle.SetMigrating(false)
    util.Mlog.Printf("rebalance done")
}

func (proxy *Proxy) RebalanceBackend(backend *Backend, circle *Circle, databases []string) {
    var migrateCount int
    defer circle.MigrateWg.Done()
    circleId := circle.CircleId
    if !backend.Active {
        util.Mlog.Printf("backend not active: %s", backend.Url)
        return
    }

    stats := proxy.MigrateStats[circleId][backend.Url]
    stats.DatabaseTotal = int32(len(databases))
    measuresOfDbs := make([][]string, len(databases))
    for i, db := range databases {
        measuresOfDbs[i] = backend.GetMeasurements(db)
        stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
    }

    for i, db := range databases {
        for j, meas := range measuresOfDbs[i] {
            key := GetKey(db, meas)
            dstBackendUrl, err := circle.Router.Get(key)
            if err != nil {
                util.Mlog.Printf("router get error: %s(%d) %s %s", circle.Name, circle.CircleId, key, err)
                continue
            }
            if dstBackendUrl != backend.Url {
                util.Mlog.Printf("src:%s dst:%s db:%s measurement:%s", backend.Url, dstBackendUrl, db, meas)
                dstBackend := circle.UrlToBackend[dstBackendUrl]
                migrateCount++
                circle.BackendWgMap[backend.Url].Add(1)
                go proxy.Migrate(backend, []*Backend{dstBackend}, circle, db, meas, 0)
                if migrateCount % proxy.MigrateCpus == 0 || (i + 1 == len(databases) && j + 1 == len(measuresOfDbs[i])) {
                    circle.BackendWgMap[backend.Url].Wait()
                }
                atomic.AddInt32(&stats.MigrateCount, 1)
            } else {
                atomic.AddInt32(&stats.InPlaceCount, 1)
            }
            atomic.AddInt32(&stats.MeasurementDone, 1)
        }
        atomic.AddInt32(&stats.DatabaseDone, 1)
    }
}

func (proxy *Proxy) Recovery(fromCircleId, toCircleId int, recoveryUrls []string, databases []string) {
    util.SetMLog(proxy.MlogDir, "recovery.log")
    util.Mlog.Printf("recovery start")
    fromCircle := proxy.Circles[fromCircleId]
    toCircle := proxy.Circles[toCircleId]

    toCircle.SetMigrating(true)
    if len(databases) == 0 {
        databases = proxy.GetDatabases()
    }
    recoveryUrlMap := make(map[string]bool)
    if len(recoveryUrls) != 0 {
        for _, u := range recoveryUrls {
            recoveryUrlMap[u] = true
        }
    } else {
        for u := range toCircle.UrlToBackend {
            recoveryUrlMap[u] = true
        }
    }
    proxy.ClearMigrateStats()
    for _, backend := range fromCircle.Backends {
        fromCircle.MigrateWg.Add(1)
        go proxy.RecoveryBackend(backend, fromCircle, toCircle, recoveryUrlMap, databases)
    }
    fromCircle.MigrateWg.Wait()
    defer toCircle.SetMigrating(false)
    util.Mlog.Printf("recovery done")
}

func (proxy *Proxy) RecoveryBackend(backend *Backend, fromCircle, toCircle *Circle, recoveryUrlMap map[string]bool, databases []string) {
    var migrateCount int
    defer fromCircle.MigrateWg.Done()
    fromCircleId := fromCircle.CircleId
    if !backend.Active {
        util.Mlog.Printf("backend not active: %s", backend.Url)
        return
    }

    stats := proxy.MigrateStats[fromCircleId][backend.Url]
    stats.DatabaseTotal = int32(len(databases))
    measuresOfDbs := make([][]string, len(databases))
    for i, db := range databases {
        measuresOfDbs[i] = backend.GetMeasurements(db)
        stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
    }

    for i, db := range databases {
        for j, meas := range measuresOfDbs[i] {
            key := GetKey(db, meas)
            dstBackendUrl, err := toCircle.Router.Get(key)
            if err != nil {
                util.Mlog.Printf("router get error: %s(%d) %s(%d) %s %s", fromCircle.Name, fromCircle.CircleId, toCircle.Name, toCircle.CircleId, key, err)
                continue
            }
            if _, ok := recoveryUrlMap[dstBackendUrl]; ok {
                util.Mlog.Printf("src:%s dst:%s db:%s measurement:%s", backend.Url, dstBackendUrl, db, meas)
                dstBackend := toCircle.UrlToBackend[dstBackendUrl]
                migrateCount++
                fromCircle.BackendWgMap[backend.Url].Add(1)
                go proxy.Migrate(backend, []*Backend{dstBackend}, fromCircle, db, meas, 0)
                if migrateCount % proxy.MigrateCpus == 0 || (i + 1 == len(databases) && j + 1 == len(measuresOfDbs[i])) {
                    fromCircle.BackendWgMap[backend.Url].Wait()
                }
                atomic.AddInt32(&stats.MigrateCount, 1)
            } else {
                atomic.AddInt32(&stats.InPlaceCount, 1)
            }
            atomic.AddInt32(&stats.MeasurementDone, 1)
        }
        atomic.AddInt32(&stats.DatabaseDone, 1)
    }
}

func (proxy *Proxy) Resync(databases []string, seconds int) {
    util.SetMLog(proxy.MlogDir, "resync.log")
    util.Mlog.Printf("resync start")
    if len(databases) == 0 {
        databases = proxy.GetDatabases()
    }
    proxy.SetResyncing(true)
    proxy.ClearMigrateStats()
    for _, circle := range proxy.Circles {
        for _, backend := range circle.Backends {
            circle.MigrateWg.Add(1)
            go proxy.ResyncBackend(backend, circle, databases, seconds)
        }
        circle.MigrateWg.Wait()
        util.Mlog.Printf("circle %d resync done", circle.CircleId)
    }
    defer proxy.SetResyncing(false)
    util.Mlog.Printf("resync done")
}

func (proxy *Proxy) ResyncBackend(backend *Backend, circle *Circle, databases []string, seconds int) {
    var migrateCount int
    defer circle.MigrateWg.Done()
    circleId := circle.CircleId
    if !backend.Active {
        util.Mlog.Printf("backend not active: %s", backend.Url)
        return
    }

    stats := proxy.MigrateStats[circleId][backend.Url]
    stats.DatabaseTotal = int32(len(databases))
    measuresOfDbs := make([][]string, len(databases))
    for i, db := range databases {
        measuresOfDbs[i] = backend.GetMeasurements(db)
        stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
    }

    for i, db := range databases {
        for j, meas := range measuresOfDbs[i] {
            key := GetKey(db, meas)
            dstBackends := make([]*Backend, 0)
            for _, toCircle := range proxy.Circles {
                if toCircle.CircleId != circleId {
                    dstBackendUrl, err := toCircle.Router.Get(key)
                    if err != nil {
                        util.Mlog.Printf("router get error: %s(%d) %s(%d) %s %s", circle.Name, circle.CircleId, toCircle.Name, toCircle.CircleId, key, err)
                        continue
                    }
                    dstBackend := toCircle.UrlToBackend[dstBackendUrl]
                    dstBackends = append(dstBackends, dstBackend)
                }
            }
            if len(dstBackends) > 0 {
                util.Mlog.Printf("src:%s dst:%v db:%s measurement:%s", backend.Url, proxy.GetBackendUrls(dstBackends), db, meas)
                migrateCount++
                circle.BackendWgMap[backend.Url].Add(1)
                go proxy.Migrate(backend, dstBackends, circle, db, meas, seconds)
                if migrateCount % proxy.MigrateCpus == 0 || (i + 1 == len(databases) && j + 1 == len(measuresOfDbs[i])) {
                    circle.BackendWgMap[backend.Url].Wait()
                }
                atomic.AddInt32(&stats.MigrateCount, 1)
            } else {
                atomic.AddInt32(&stats.InPlaceCount, 1)
            }
            atomic.AddInt32(&stats.MeasurementDone, 1)
        }
        atomic.AddInt32(&stats.DatabaseDone, 1)
    }
}

func (proxy *Proxy) Clear(circleId int) {
    util.SetMLog(proxy.MlogDir, "clear.log")
    util.Mlog.Printf("clear start")
    circle := proxy.Circles[circleId]
    circle.SetMigrating(true)
    proxy.ClearMigrateStats()
    for _, backend := range circle.Backends {
        circle.MigrateWg.Add(1)
        go proxy.ClearBackend(backend, circle)
    }
    circle.MigrateWg.Wait()
    defer circle.SetMigrating(false)
    util.Mlog.Printf("clear done")
}

func (proxy *Proxy) ClearBackend(backend *Backend, circle *Circle) {
    var migrateCount int
    defer circle.MigrateWg.Done()
    circleId := circle.CircleId
    if !backend.Active {
        util.Mlog.Printf("backend not active: %s", backend.Url)
        return
    }

    databases := backend.GetDatabases()
    stats := proxy.MigrateStats[circleId][backend.Url]
    stats.DatabaseTotal = int32(len(databases))
    measuresOfDbs := make([][]string, len(databases))
    for i, db := range databases {
        measuresOfDbs[i] = backend.GetMeasurements(db)
        stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
    }

    for i, db := range databases {
        for j, meas := range measuresOfDbs[i] {
            util.Mlog.Printf("check backend:%s db:%s measurement:%s", backend.Url, db, meas)
            key := GetKey(db, meas)
            dstBackendUrl, err := circle.Router.Get(key)
            if err != nil {
                util.Mlog.Printf("router get error: %s(%d) %s %s", circle.Name, circle.CircleId, key, err)
                continue
            }
            if dstBackendUrl != backend.Url {
                util.Mlog.Printf("backend:%s db:%s measurement:%s should migrate to %s", backend.Url, db, meas, dstBackendUrl)
                migrateCount++
                circle.BackendWgMap[backend.Url].Add(1)
                go func(backend *Backend, circle *Circle, db, meas string) {
                    _, err := backend.DropMeasurement(db, meas)
                    if err == nil {
                        util.Mlog.Printf("clear backend:%s db:%s measurement:%s done", backend.Url, db, meas)
                    } else {
                        util.Mlog.Printf("clear backend:%s db:%s measurement:%s error: %s", backend.Url, db, meas, err)
                    }
                    defer circle.BackendWgMap[backend.Url].Done()
                }(backend, circle, db, meas)
                if migrateCount % proxy.MigrateCpus == 0 || (i + 1 == len(databases) && j + 1 == len(measuresOfDbs[i])) {
                    circle.BackendWgMap[backend.Url].Wait()
                }
                atomic.AddInt32(&stats.MigrateCount, 1)
            } else {
                atomic.AddInt32(&stats.InPlaceCount, 1)
            }
            atomic.AddInt32(&stats.MeasurementDone, 1)
        }
        atomic.AddInt32(&stats.DatabaseDone, 1)
    }
}

func (proxy *Proxy) ClearMigrateStats() {
    for _, stats := range proxy.MigrateStats {
        for _, mi := range stats {
            mi.DatabaseTotal = 0
            mi.DatabaseDone = 0
            mi.MeasurementTotal = 0
            mi.MeasurementDone = 0
            mi.MigrateCount = 0
            mi.InPlaceCount = 0
        }
    }
}

func (proxy *Proxy) SetResyncing(resyncing bool) {
    proxy.Lock.Lock()
    defer proxy.Lock.Unlock()
    proxy.IsResyncing = resyncing
}
