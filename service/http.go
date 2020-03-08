package service

import (
    "bytes"
    "compress/gzip"
    "encoding/json"
    "github.com/chengshiwen/influx-proxy/consistent"
    "github.com/chengshiwen/influx-proxy/util"
    "io/ioutil"
    "math/rand"
    "net/http"
    "net/http/pprof"
    "strconv"
    "strings"
    "sync"
    "time"
)

type HttpService struct {
    *consistent.Proxy
}

// Register 注册http方法
func (hs *HttpService) Register(mux *http.ServeMux) {
    mux.HandleFunc("/encrypt", hs.HandlerEncrypt)
    mux.HandleFunc("/decrypt", hs.HandlerDencrypt)
    mux.HandleFunc("/ping", hs.HandlerPing)
    mux.HandleFunc("/query", hs.HandlerQuery)
    mux.HandleFunc("/write", hs.HandlerWrite)
    mux.HandleFunc("/clear", hs.HandlerClear)
    mux.HandleFunc("/set_migrate_flag", hs.HandlerSetMigrateFlag)
    mux.HandleFunc("/get_migrate_flag", hs.HandlerGetMigrateFlag)
    mux.HandleFunc("/rebalance", hs.HandlerRebalance)
    mux.HandleFunc("/recovery", hs.HandlerRecovery)
    mux.HandleFunc("/resync", hs.HandlerResync)
    mux.HandleFunc("/status", hs.HandlerStatus)
    mux.HandleFunc("/debug/pprof/", pprof.Index)
    mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
    return
}

func (hs *HttpService)HandlerEncrypt(w http.ResponseWriter, req *http.Request)  {
    defer req.Body.Close()
    if req.Method != http.MethodGet {
        w.WriteHeader(405)
        w.Write([]byte("method not allow\n"))
        return
    }
    ctx := req.URL.Query().Get("ctx")
    encrypt := util.AesEncrypt(ctx, util.CipherKey)
    w.WriteHeader(200)
    w.Write([]byte(encrypt))
}

func (hs *HttpService)HandlerDencrypt(w http.ResponseWriter, req *http.Request)  {
    defer req.Body.Close()
    if req.Method != http.MethodGet {
        w.WriteHeader(405)
        w.Write([]byte("method not allow\n"))
        return
    }
    key := req.URL.Query().Get("key")
    ctx := req.URL.Query().Get("ctx")
    decrypt := util.AesDecrypt(ctx, key)
    w.WriteHeader(200)
    w.Write([]byte(decrypt))
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)
    w.WriteHeader(http.StatusNoContent)
    return
}

// HandlerQuery query方法入口
func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    // 检查认证
    if !hs.checkAuth(req) {
        w.WriteHeader(401)
        w.Write([]byte("authentication failed\n"))
        return
    }

    // 检查请求方法
    if req.Method != http.MethodPost && req.Method != http.MethodGet {
        util.Log.Errorf("method:%+v", req.Method)
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("illegal method\n"))
        return
    }

    // 检查查询语句
    q := strings.TrimSpace(req.FormValue("q"))
    if q == "" {
        util.Log.Errorf("query:%+v", q)
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("empty query\n"))
        return
    }

    // 选出一个状态良好的cluster
    var circle *consistent.Circle
    for {
        num := rand.Intn(len(hs.Circles))
        circle = hs.Circles[num]
        if circle.ReadyMigrating {
            continue
        }
        if circle.CheckStatus() {
            break
        }
        time.Sleep(time.Microsecond)
    }

    // 检查带measurement的查询语句
    err := hs.CheckMeasurementQuery(q)
    if err != nil {
        // 检查集群查询语句，如show measurements
        err = hs.CheckClusterQuery(q)
        if err == nil {
            body, err := circle.QueryCluster(req, circle.Backends)
            if err != nil {
                util.Log.Errorf("query cluster:%+v err:%+v", q, err)
                w.WriteHeader(http.StatusBadRequest)
                w.Write([]byte(err.Error()))
                return
            }
            w.WriteHeader(http.StatusOK)
            w.Write(body)
            return
        }
        w.WriteHeader(400)
        w.Write([]byte("query forbidden\n"))
        return
    }

    // 执行查询
    resp, err := circle.Query(req)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(err.Error()))
        return
    }
    w.WriteHeader(http.StatusOK)
    w.Write(resp)
    return
}

// HandlerWrite write方法入口
func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    // 检查认证
    if !hs.checkAuth(req) {
        w.WriteHeader(401)
        w.Write([]byte("authentication failed\n"))
        return
    }

    // 判断http方法
    if req.Method != http.MethodPost {
        util.Log.Errorf("req.Method:%+v err:nil", req.Method)
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }

    // precision默认为ns
    precision := req.URL.Query().Get("precision")
    if precision == "" {
        precision = "ns"
    }
    // db必须要给出
    db := req.URL.Query().Get("db")
    if db == "" {
        w.WriteHeader(http.StatusBadRequest)
        w.Write(util.StatusText(http.StatusBadRequest))
        return
    }
    if !util.ContainString(hs.DbList, db) {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("database not exist\n"))
        return
    }

    body := req.Body
    // 压缩请求数据
    if req.Header.Get("Content-Encoding") == "gzip" {
        b, err := gzip.NewReader(body)
        defer b.Close()
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
            return
        }
        body = b
    }
    // 读出请求数据
    p, err := ioutil.ReadAll(body)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(err.Error()))
        return
    }

    // 多个对象，遍历每一个对象
    arr := bytes.Split(p, []byte("\n"))
    for _, line := range arr {
        // 数据对象格式是否正确
        sections := bytes.Split(line, []byte(" "))
        if len(line) == 0 || len(sections) < 2 {
            continue
        }
        items := bytes.Split(sections[0], []byte(","))
        // 构建一个数据对象
        data := &consistent.LineData{
            Precision: precision,
            Line:      line,
            Db:        db,
            Measure:   string(items[0]),
        }
        // 写入buffer
        err = hs.WriteData(data)
        if err != nil {
            util.Log.Errorf("request data:%+v err:%+v", data, err)
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
            return
        }
    }
    w.WriteHeader(http.StatusNoContent)
    return
}

func (hs *HttpService) HandlerClear(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        util.Log.Errorf("req.Method:%+v err:nil", req.Method)
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }
    circleNum, err := strconv.Atoi(req.FormValue("circle_num"))
    if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    db := strings.Trim(req.FormValue("db"), ",")
    if db == "" {
        w.WriteHeader(http.StatusBadRequest)
        w.Write(util.StatusText(http.StatusBadRequest))
        return
    }
    dbs := strings.Split(db, ",")
    go hs.Clear(dbs, circleNum)

    w.WriteHeader(http.StatusOK)
    w.Write(util.StatusText(http.StatusOK))
    return
}

func (hs *HttpService) HandlerSetMigrateFlag(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }

    circleNumStr := req.FormValue("circle_num")
    circleNumStrs := strings.Split(circleNumStr, ",")

    flagStr := req.FormValue("flag")
    flagStrs := strings.Split(flagStr, ",")
    if len(circleNumStrs) != len(flagStrs) {
        w.WriteHeader(http.StatusBadRequest)
        w.Write(util.StatusText(http.StatusBadRequest))
        return
    }

    for k, v := range circleNumStrs {
        circleNum, err := strconv.Atoi(v)
        if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte("invalid circle_num\n"))
            return
        }

        flag, err := strconv.ParseBool(flagStrs[k])
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
            return
        }

        hs.Circles[circleNum].SetReadyMigrating(flag)
    }

    w.WriteHeader(http.StatusOK)
    w.Write(util.StatusText(http.StatusOK))
}

func (hs *HttpService) HandlerGetMigrateFlag(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    resp := make([]*consistent.MigrateFlagStatus, len(hs.Circles))
    for k, v := range hs.Circles {
        resp[k] = &consistent.MigrateFlagStatus{
            ReadyMigratingFlag: v.ReadyMigrating,
            IsMigratingFlag: v.IsMigrating,
        }
    }

    respData, _ := json.Marshal(resp)
    w.WriteHeader(http.StatusOK)
    w.Write(respData)
}

func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }

    operateType := req.FormValue("operate_type")
    if operateType != "add" && operateType != "del" {
        w.WriteHeader(http.StatusBadRequest)
        w.Write(util.StatusText(http.StatusBadRequest))
        return
    }
    circleNum, err := strconv.Atoi(req.FormValue("circle_num"))
    if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    db := strings.Trim(req.FormValue("db"), ",")
    var dbs []string
    if db != "" {
        dbs = strings.Split(db, ",")
    }

    // add or del backend in circle
    var backends []*consistent.Backend
    if operateType == "add" {
        // 当前所有的实例列表
        backends, err = hs.AddBackend(circleNum)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
            return
        }
    } else if operateType == "del" {
        backendUrls := strings.Split(strings.Trim(req.FormValue("backends"), ","), ",")
        if len(backendUrls) == 0 {
            w.WriteHeader(http.StatusBadRequest)
            w.Write(util.StatusText(http.StatusBadRequest))
            return
        }
        // 要删除的实例列表
        backends, err = hs.DeleteBackend(backendUrls)
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
            return
        }
        cpuCores, err := strconv.Atoi(req.FormValue("cpu_cores"))
        if err != nil {
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
            return
        }

        // backends也已经删除需要创建一个进度状态信息
        for _, backend := range backends {
            hs.BackendRebalanceStatus[circleNum][backend.Url] = &consistent.MigrationInfo{}
            hs.Circles[circleNum].BackendWgMap[backend.Url] = &sync.WaitGroup{}
            backend.MigrateCpuCores = cpuCores
        }
        for _, backend := range hs.Circles[circleNum].Backends {
            backends = append(backends, backend)
        }
    }

    // 判断迁移是否已就绪
    if !hs.Circles[circleNum].ReadyMigrating {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("call all proxy to set_migrate_flag\n"))
        return
    }

    // 判断是否正在迁移
    if hs.Circles[circleNum].GetIsMigrating() {
        w.WriteHeader(http.StatusAccepted)
        w.Write(util.StatusText(http.StatusAccepted))
        return
    }
    // rebalance
    go hs.Rebalance(backends, circleNum, dbs)
    w.WriteHeader(http.StatusOK)
    w.Write(util.StatusText(http.StatusOK))
    return
}

func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }

    fromCircleNum, err := strconv.Atoi(req.FormValue("from_circle_num"))
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(err.Error()))
        return
    }
    toCircleNum, err := strconv.Atoi(req.FormValue("to_circle_num"))
    if err != nil {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte(err.Error()))
        return
    }
    if fromCircleNum < 0 || fromCircleNum >= len(hs.Circles) || toCircleNum < 0 || toCircleNum >= len(hs.Circles) || fromCircleNum == toCircleNum {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("invalid circle_num\n"))
        return
    }

    db := strings.Trim(req.FormValue("db"), ",")
    var dbs []string
    if db != "" {
        dbs = strings.Split(db, ",")
    }

    // 判断迁移是否已就绪
    if !hs.Circles[toCircleNum].ReadyMigrating {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("call all proxy to set_migrate_flag\n"))
        return
    }

    if hs.Circles[fromCircleNum].GetIsMigrating() || hs.Circles[toCircleNum].GetIsMigrating() {
        w.WriteHeader(http.StatusAccepted)
        w.Write(util.StatusText(http.StatusAccepted))
        return
    }

    backendUrls := strings.Split(strings.Trim(req.FormValue("fault_backends"), ","), ",")
    if len(backendUrls) == 0 {
        w.WriteHeader(http.StatusBadRequest)
        w.Write(util.StatusText(http.StatusBadRequest))
        return
    }

    go hs.Recovery(fromCircleNum, toCircleNum, backendUrls, dbs)
    w.WriteHeader(http.StatusOK)
    w.Write(util.StatusText(http.StatusOK))
    return
}

func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }

    db := strings.Trim(req.FormValue("db"), ",")
    var dbs []string
    if db != "" {
        dbs = strings.Split(db, ",")
    }

    lastSecondsStr := req.FormValue("last_seconds")
    if lastSecondsStr == "" {
        lastSecondsStr = "0"
    }
    lastSeconds, err := strconv.Atoi(lastSecondsStr)
    if err != nil || lastSeconds < 0 {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("invalid latest_seconds\n"))
        return
    }

    for _, circle := range hs.Circles {
        if circle.GetIsMigrating() {
            w.WriteHeader(http.StatusAccepted)
            w.Write(util.StatusText(http.StatusAccepted))
            return
        }
    }

    go hs.Resync(dbs, lastSeconds)
    w.WriteHeader(http.StatusOK)
    w.Write(util.StatusText(http.StatusOK))
    return
}

func (hs *HttpService) HandlerStatus(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodGet {
        w.WriteHeader(http.StatusMethodNotAllowed)
        w.Write(util.StatusText(http.StatusMethodNotAllowed))
        return
    }

    circleNumStr := req.FormValue("circle_num")
    circleNum, err := strconv.Atoi(circleNumStr)
    if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    var res []byte
    statusType := req.FormValue("type")
    if statusType == "rebalance" {
        res, err = json.Marshal(hs.BackendRebalanceStatus[circleNum])
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
        }
    } else if statusType == "recovery" {
        res, err = json.Marshal(hs.BackendRecoveryStatus[circleNum])
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
        }
    } else if statusType == "resync" {
        res, err = json.Marshal(hs.BackendResyncStatus[circleNum])
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            w.WriteHeader(http.StatusBadRequest)
            w.Write([]byte(err.Error()))
        }
    } else {
        w.WriteHeader(http.StatusBadRequest)
        w.Write([]byte("invalid status type\n"))
        return
    }

    w.WriteHeader(http.StatusOK)
    w.Write(res)
    return
}

func (hs *HttpService) AddHeader(w http.ResponseWriter) {
    w.Header().Add("X-Influxdb-Version", util.Version)
}

func (hs *HttpService) checkAuth(r *http.Request) bool {
    if hs.Username == "" && hs.Password == "" {
        return true
    }
    u, p := r.URL.Query().Get("u"), r.URL.Query().Get("p")
    if util.AesEncrypt(u, util.CipherKey) == hs.Username && util.AesEncrypt(p, util.CipherKey) == hs.Password  {
        return true
    }
    u, p, ok := r.BasicAuth()
    if ok && util.AesEncrypt(u, util.CipherKey) == hs.Username && util.AesEncrypt(p, util.CipherKey) == hs.Password {
        return true
    }
    return false
}
