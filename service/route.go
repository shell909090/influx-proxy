package service

import (
    "bytes"
    "compress/gzip"
    "encoding/json"
    "github.com/chengshiwen/influx-proxy/consist"
    "github.com/chengshiwen/influx-proxy/util"
    "io/ioutil"
    "math/rand"
    "net/http"
    "net/http/pprof"
    "strconv"
    "strings"
    "time"
)

type HttpService struct {
    *consist.Proxy
}

// Register 注册http方法
func (hs *HttpService) Register(mux *http.ServeMux) {
    mux.HandleFunc("/encryption", hs.HandlerEncryption)
    mux.HandleFunc("/decryption", hs.HandlerDencryption)
    mux.HandleFunc("/query", hs.HandlerQuery)
    mux.HandleFunc("/write", hs.HandlerWrite)
    mux.HandleFunc("/clear_measure", hs.HandlerClearMeasure)
    mux.HandleFunc("/set_migrate_flag", hs.HandlerSetMigrateFlag)
    mux.HandleFunc("/get_migrate_flag", hs.HandlerGetMigrateFlag)
    mux.HandleFunc("/debug/pprof/", pprof.Index)
    mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
    return
}

func (hs *HttpService)HandlerEncryption(w http.ResponseWriter, req *http.Request)  {
    defer req.Body.Close()
    if req.Method != "GET" {
        w.WriteHeader(405)
        w.Write([]byte("method not allow."))
        return
    }
    ctx := req.URL.Query().Get("ctx")
    passord := util.AesEncrypt(ctx, consist.KEY)
    w.WriteHeader(200)
    w.Write([]byte(passord))
}

func (hs *HttpService)HandlerDencryption(w http.ResponseWriter, req *http.Request)  {
    defer req.Body.Close()
    if req.Method != "GET" {
        w.WriteHeader(405)
        w.Write([]byte("method not allow."))
        return
    }
    key := req.URL.Query().Get("key")
    ctx := req.URL.Query().Get("ctx")
    passord := util.AesDecrypt(ctx, key)
    w.WriteHeader(200)
    w.Write([]byte(passord))
}

// HandlerQuery query方法入口
func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.WriteHeader(w, req)
    w.Header().Add("X-Influxdb-Version", util.Version)

    // 验证密码
    ok := hs.checkAuth(req)
    if !ok{
        w.WriteHeader(401)
        w.Write([]byte("auth failed"))
        return
    }

    // 检查请求方法
    if !util.ContainString([]string{util.Post, util.Get}, req.Method) {
        util.Log.Errorf("method:%+v", req.Method)
        w.WriteHeader(util.BadRequest)
        w.Write([]byte("illegal method\n"))
        return
    }

    // 检查查询语句
    q := strings.TrimSpace(req.FormValue("q"))
    if q == "" {
        util.Log.Errorf("query:%+v", q)
        w.WriteHeader(util.BadRequest)
        w.Write([]byte("empty query\n"))
        return
    }

    // 选出一个状态良好的cluster
    var circle *consist.Circle
    for {
        randClusterPos := rand.Intn(len(hs.Circles))
        circle = hs.Circles[randClusterPos]
        if circle.ReadyMigrating {
            continue
        }
        status := circle.CheckStatus()
        if status {
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
                w.WriteHeader(util.BadRequest)
                w.Write([]byte(err.Error()))
                return
            }
            w.WriteHeader(util.Success)
            w.Write(body)
            return
        }
        w.WriteHeader(400)
        w.Write([]byte("query forbidden"))
        return
    }

    // 执行查询
    resp, e := circle.Query(req)
    if e != nil {
        util.Log.Errorf("err:%+v", e)
        w.WriteHeader(util.BadRequest)
        w.Write([]byte(e.Error()))
        return
    }
    w.WriteHeader(util.Success)
    w.Write(resp)
    return
}

// HandlerWrite write方法入口
func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.WriteHeader(w, req)

    ok := hs.checkAuth(req)
    if !ok{
        w.WriteHeader(401)
        w.Write([]byte("auth failed"))
        return
    }

    // 判断http方法
    if req.Method != util.Post {
        util.Log.Errorf("req.Method:%+v err:nil", req.Method)
        w.WriteHeader(util.MethodNotAllow)
        w.Write(util.Code2Message[util.MethodNotAllow])
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
        w.WriteHeader(util.BadRequest)
        w.Write(util.Code2Message[util.BadRequest])
        return
    }
    if !util.ContainString(hs.DbList, db) {
        w.WriteHeader(util.BadRequest)
        w.Write([]byte("database not found"))
        return
    }

    body := req.Body
    // 压缩请求数据
    if req.Header.Get("Content-Encoding") == "gzip" {
        b, err := gzip.NewReader(body)
        defer b.Close()
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            w.WriteHeader(util.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }
        body = b
    }
    // 读出请求数据
    p, err := ioutil.ReadAll(body)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        w.WriteHeader(util.BadRequest)
        w.Write([]byte(err.Error()))
        return
    }

    // 多个对象，遍历每一个对象
    arr := bytes.Split(p, []byte("\n"))
    for _, line := range arr {
        // 数据对象格式是否正确
        if len(line) == 0 {
            continue
        }
        lineList := bytes.Split(line, []byte(" "))
        if len(lineList) < 2 {
            w.WriteHeader(util.BadRequest)
            w.Write(util.Code2Message[util.BadRequest])
        }
        measure := lineList[0]
        measures := bytes.Split(measure, []byte(","))

        // 构建一个数据对象
        data := &consist.LineData{
            Precision: precision,
            Line:      line,
            Db:        db,
            Measure:   string(measures[0]),
        }
        // 写入buffer
        err = hs.WriteData(data)
        if err != nil {
            util.Log.Errorf("request data:%+v err:%+v", data, err)
            w.WriteHeader(util.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }
    }
    w.WriteHeader(util.SuccessNoResp)
    return
}

func (hs *HttpService) HandlerClearMeasure(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.WriteHeader(w, req)
    if req.Method != util.Post {
        util.Log.Errorf("req.Method:%+v err:nil", req.Method)
        w.WriteHeader(util.MethodNotAllow)
        w.Write(util.Code2Message[util.MethodNotAllow])
        return
    }
    db := req.FormValue("db")
    dbs := strings.Split(db, ",")
    circleNum, e := strconv.Atoi(req.FormValue("circle_num"))
    if e != nil || circleNum >= len(hs.Circles) || len(dbs) == 0 {
        w.WriteHeader(util.BadRequest)
        w.Write([]byte(e.Error()))
        return
    }
    go hs.ClearMeasure(dbs, circleNum)

    w.WriteHeader(util.Success)
    w.Write(util.Code2Message[util.Success])
    return
}

func (hs *HttpService) HandlerSetMigrateFlag(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    w.Header().Add("X-Influxdb-Version", util.Version)

    if req.Method != util.Post {
        w.WriteHeader(util.MethodNotAllow)
        w.Write(util.Code2Message[util.MethodNotAllow])
        return
    }

    circleNumStr := req.FormValue("circle_num")
    circleNumStrs := strings.Split(circleNumStr, ",")

    flagStr := req.FormValue("flag")
    flagStrs := strings.Split(flagStr, ",")
    if len(circleNumStrs) != len(flagStrs) {
        w.WriteHeader(util.BadRequest)
        w.Write(util.Code2Message[util.BadRequest])
        return
    }

    for k, v := range circleNumStrs {
        circleNum, err := strconv.Atoi(v)
        if err != nil || circleNum > len(hs.Circles) {
            w.WriteHeader(util.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }

        flag, err := strconv.ParseBool(flagStrs[k])
        if err != nil {
            w.WriteHeader(util.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }

        hs.Circles[circleNum].SetReadyMigrating(flag)
    }

    w.WriteHeader(util.SuccessNoResp)
}

func (hs *HttpService) HandlerGetMigrateFlag(w http.ResponseWriter, req *http.Request) {
    resp := make([]*consist.MigrateFlagStatus, len(hs.Circles))
    for k, v := range hs.Circles {
        resp[k] = &consist.MigrateFlagStatus{
            ReadyMigratingFlag: v.ReadyMigrating,
            IsMigratingFlag: v.IsMigrating,
        }
    }

    respData, err := json.Marshal(resp)
    if err != nil {
        w.WriteHeader(util.SuccessNoResp)
        return
    }

    w.WriteHeader(util.Success)
    w.Write(respData)
}

func (hs *HttpService) WriteHeader(w http.ResponseWriter, req *http.Request) {
    w.Header().Add("X-Influxdb-Version", util.Version)
}

func (hs *HttpService) checkAuth(r *http.Request) bool {
    userName, password, ok := r.BasicAuth()
    if ok && util.AesEncrypt(userName, consist.KEY) == hs.ProxyUsername && util.AesEncrypt(password, consist.KEY) == hs.ProxyPassword {
        return true
    }

    userName, password = r.URL.Query().Get("u"), r.URL.Query().Get("p")
    if util.AesEncrypt(userName, consist.KEY) == hs.ProxyUsername && util.AesEncrypt(password, consist.KEY) == hs.ProxyPassword  {
        return true
    }
    return false
}
