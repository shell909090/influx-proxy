package service

import (
    "bytes"
    "compress/gzip"
    "github.com/chengshiwen/influx-proxy/consist"
    "github.com/chengshiwen/influx-proxy/mconst"
    "github.com/chengshiwen/influx-proxy/util"
    "io/ioutil"
    "math/rand"
    "net/http"
    "net/http/pprof"
    "regexp"
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
    passord := consist.AesEncrypt(ctx, consist.KEY)
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
    passord := consist.AesDecrypt(ctx, key)
    w.WriteHeader(200)
    w.Write([]byte(passord))
}

// HandlerQuery query方法入口
func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.WriteHeader(w, req)
    w.Header().Add("X-Influxdb-Version", mconst.Version)

    // 验证密码
    ok := hs.checkAuth(req)
    if !ok{
        w.WriteHeader(401)
        w.Write([]byte("auth failed"))
        return
    }

    // 检查请求方法
    if !util.IncludeString([]string{mconst.Post, mconst.Get}, req.Method) {
        util.CustomLog.Errorf("method:%+v", req.Method)
        w.WriteHeader(mconst.BadRequest)
        w.Write([]byte("illegal method\n"))
        return
    }

    // 检查查询语句
    q := strings.TrimSpace(req.FormValue("q"))
    if q == "" {
        util.CustomLog.Errorf("query:%+v", q)
        w.WriteHeader(mconst.BadRequest)
        w.Write([]byte("empty query\n"))
        return
    }

    // 选出一个状态良好的cluster
    var circle *consist.Circle
    for {
        randClusterPos := rand.Intn(len(hs.Circles))
        circle = hs.Circles[randClusterPos]
        status := circle.CheckStatus()
        if status {
            break
        }
        time.Sleep(time.Microsecond)
    }

    // 筛选出 show 操作
    matched := MatchShow(q)
    if matched {
        body, err := circle.QueryShow(req, circle.Backends)
        if err != nil {
            util.CustomLog.Errorf("query:%+v err:%+v", q, err)
            w.WriteHeader(mconst.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }
        w.WriteHeader(mconst.Success)
        w.Write(body)
        return
    }

    // 过滤查询语句
    err := hs.CheckQuery(q)
    if err != nil {
        w.WriteHeader(mconst.BadRequest)
        w.Write([]byte(err.Error()))
        return
    }

    // 执行查询
    resp, e := circle.Query(req)
    if e != nil {
        util.CustomLog.Errorf("err:%+v", e)
        w.WriteHeader(mconst.BadRequest)
        w.Write([]byte(e.Error()))
        return
    }
    w.WriteHeader(mconst.Success)
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
    if req.Method != mconst.Post {
        util.CustomLog.Errorf("req.Method:%+v err:nil", req.Method)
        w.WriteHeader(mconst.MethodNotAllow)
        w.Write(mconst.Code2Message[mconst.MethodNotAllow])
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
        w.WriteHeader(mconst.BadRequest)
        w.Write(mconst.Code2Message[mconst.BadRequest])
        return
    }

    body := req.Body
    // 压缩请求数据
    if req.Header.Get("Content-Encoding") == "gzip" {
        b, err := gzip.NewReader(body)
        defer b.Close()
        if err != nil {
            util.CustomLog.Errorf("err:%+v", err)
            w.WriteHeader(mconst.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }
        body = b
    }
    // 读出请求数据
    p, err := ioutil.ReadAll(body)
    if err != nil {
        util.CustomLog.Errorf("err:%+v", err)
        w.WriteHeader(mconst.BadRequest)
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
            w.WriteHeader(mconst.BadRequest)
            w.Write(mconst.Code2Message[mconst.BadRequest])
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
            util.CustomLog.Errorf("request data:%+v err:%+v", data, err)
            w.WriteHeader(mconst.BadRequest)
            w.Write([]byte(err.Error()))
            return
        }
    }
    w.WriteHeader(mconst.SuccessNoResp)
    return
}

func (hs *HttpService) HandlerClearMeasure(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.WriteHeader(w, req)
    if req.Method != mconst.Post {
        util.CustomLog.Errorf("req.Method:%+v err:nil", req.Method)
        w.WriteHeader(mconst.MethodNotAllow)
        w.Write(mconst.Code2Message[mconst.MethodNotAllow])
        return
    }
    db := req.FormValue("db")
    dbs := strings.Split(db, ",")
    circleNum, e := strconv.Atoi(req.FormValue("circle_num"))
    if e != nil || circleNum >= len(hs.Circles) || len(dbs) == 0 {
        w.WriteHeader(mconst.BadRequest)
        w.Write([]byte(e.Error()))
        return
    }
    go hs.ClearMeasure(dbs, circleNum)

    w.WriteHeader(mconst.Success)
    w.Write(mconst.Code2Message[mconst.Success])
    return

}

func (hs *HttpService) WriteHeader(w http.ResponseWriter, req *http.Request) {
    w.Header().Add("X-Influxdb-Version", mconst.Version)
}

func MatchShow(q string) bool {
    res, err := regexp.MatchString("^show", q)
    if err != nil {
        util.CustomLog.Errorf("query:%+v err:%+v", q, err)
        return false
    }
    return res
}

func (hs *HttpService) checkAuth(r *http.Request) bool {
    userName, password, ok := r.BasicAuth()
    if ok && consist.AesEncrypt(userName, consist.KEY) == hs.ProxyUsername && consist.AesEncrypt(password, consist.KEY) == hs.ProxyPassword {
        return true
    }

    userName, password = r.URL.Query().Get("u"), r.URL.Query().Get("p")
    if consist.AesEncrypt(userName, consist.KEY) == hs.ProxyUsername && consist.AesEncrypt(password, consist.KEY) == hs.ProxyPassword  {
        return true
    }
    return false
}
