package service

import (
    "bytes"
    "compress/gzip"
    "encoding/json"
    "fmt"
    "github.com/chengshiwen/influx-proxy/backend"
    "github.com/chengshiwen/influx-proxy/util"
    "io/ioutil"
    "log"
    "math/rand"
    "net/http"
    "net/http/pprof"
    "strconv"
    "strings"
    "sync"
    "time"
)

type HttpService struct {
    *backend.Proxy
}

func (hs *HttpService) Register(mux *http.ServeMux) {
    mux.HandleFunc("/encrypt", hs.HandlerEncrypt)
    mux.HandleFunc("/decrypt", hs.HandlerDencrypt)
    mux.HandleFunc("/ping", hs.HandlerPing)
    mux.HandleFunc("/query", hs.HandlerQuery)
    mux.HandleFunc("/write", hs.HandlerWrite)
    mux.HandleFunc("/clear", hs.HandlerClear)
    mux.HandleFunc("/migrating", hs.HandlerMigrating)
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
        w.Write(util.StatusText(405))
        return
    }
    ctx := req.URL.Query().Get("ctx")
    encrypt := util.AesEncrypt(ctx, util.CipherKey)
    w.WriteHeader(200)
    w.Write([]byte(encrypt+"\n"))
}

func (hs *HttpService)HandlerDencrypt(w http.ResponseWriter, req *http.Request)  {
    defer req.Body.Close()
    if req.Method != http.MethodGet {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }
    key := req.URL.Query().Get("key")
    ctx := req.URL.Query().Get("ctx")
    decrypt := util.AesDecrypt(ctx, key)
    w.WriteHeader(200)
    w.Write([]byte(decrypt+"\n"))
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)
    w.WriteHeader(204)
    return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodGet && req.Method != http.MethodPost {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    if !hs.checkAuth(req) {
        w.WriteHeader(401)
        w.Write([]byte("authentication failed\n"))
        return
    }

    q := strings.TrimSpace(req.FormValue("q"))
    if q == "" {
        w.WriteHeader(400)
        w.Write([]byte("empty query\n"))
        return
    }

    db := req.URL.Query().Get("db")
    if len(hs.DbList) > 0 && !hs.checkDatabase(q) && !util.MapHasKey(hs.DbMap, db) {
        w.WriteHeader(400)
        w.Write([]byte("database not exist\n"))
        return
    }

    var circle *backend.Circle
    for {
        num := rand.Intn(len(hs.Circles))
        circle = hs.Circles[num]
        if circle.IsMigrating {
            continue
        }
        if circle.CheckStatus() {
            break
        }
        time.Sleep(time.Microsecond)
    }

    if !hs.CheckMeasurementQuery(q) {
        if hs.CheckClusterQuery(q) {
            var body []byte
            var err error
            if hs.CheckCreateDatabaseQuery(q) {
                body, err = hs.CreateDatabase(w, req)
            } else {
                body, err = circle.QueryCluster(w, req)
            }
            if err != nil {
                log.Printf("query cluster is: %s, error: %s", q, err)
                w.WriteHeader(400)
                w.Write([]byte(err.Error()))
                return
            }
            w.Write(body)
            return
        }
        w.WriteHeader(400)
        w.Write([]byte("query forbidden\n"))
        return
    }

    resp, err := circle.Query(w, req)
    if err != nil {
        log.Printf("query is: %s, error: %s", q, err)
        w.WriteHeader(400)
        w.Write([]byte(err.Error()))
        return
    }
    w.Write(resp)
    return
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    if !hs.checkAuth(req) {
        w.WriteHeader(401)
        w.Write([]byte("authentication failed\n"))
        return
    }

    precision := req.URL.Query().Get("precision")
    if precision == "" {
        precision = "ns"
    }
    db := req.URL.Query().Get("db")
    if db == "" {
        w.WriteHeader(400)
        w.Write([]byte("empty database\n"))
        return
    }
    if len(hs.DbList) > 0 && !util.MapHasKey(hs.DbMap, db) {
        w.WriteHeader(400)
        w.Write([]byte("database not exist\n"))
        return
    }

    body := req.Body
    if req.Header.Get("Content-Encoding") == "gzip" {
        b, err := gzip.NewReader(body)
        defer b.Close()
        if err != nil {
            w.Write([]byte("unable to decode gzip body\n"))
            return
        }
        body = b
    }
    p, err := ioutil.ReadAll(body)
    if err != nil {
        log.Printf("err:%+v", err)
        w.WriteHeader(400)
        w.Write([]byte(err.Error()))
        return
    }

    lines := bytes.Split(p, []byte("\n"))
    for _, line := range lines {
        if len(line) == 0 {
            continue
        }
        data := &backend.LineData{
            Db:        db,
            Line:      line,
            Precision: precision,
        }
        hs.WriteData(data)
    }
    w.WriteHeader(204)
    return
}

func (hs *HttpService) HandlerClear(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    circleNum, err := strconv.Atoi(req.FormValue("circle_num"))
    if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
        w.WriteHeader(400)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    db := strings.Trim(req.FormValue("db"), ",")
    if db == "" {
        w.WriteHeader(400)
        w.Write([]byte("empty database\n"))
        return
    }
    dbs := strings.Split(db, ",")
    go hs.Clear(dbs, circleNum)

    w.WriteHeader(202)
    w.Write([]byte("accepted\n"))
    return
}

func (hs *HttpService) HandlerMigrating(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method == http.MethodPost {
        decoder := json.NewDecoder(req.Body)
        var o struct {
            circleNum int  `json:"circle_num"`
            migrating bool `json:"migrating"`
        }
        err := decoder.Decode(&o)
        if err != nil {
            log.Printf("request body error: %s", err)
            w.Write([]byte("illegal body\n"))
            return
        }
        circle := hs.Circles[o.circleNum]
        circle.SetMigrating(o.migrating)
        res := map[string]interface{}{
            "name": circle.Name,
            "circle_num": circle.CircleNum,
            "is_migrating": circle.IsMigrating,
        }
        resData, _ := json.Marshal(res)
        w.Write(resData)
    } else if req.Method == http.MethodGet {
        res := make([]map[string]interface{}, len(hs.Circles))
        for k, circle := range hs.Circles {
            res[k] = map[string]interface{}{
                "name": circle.Name,
                "circle_num": circle.CircleNum,
                "is_migrating": circle.IsMigrating,
            }
        }
        resData, _ := json.Marshal(res)
        w.Write(resData)
    } else {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }
}

func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    operateType := req.FormValue("operate_type")
    if operateType != "add" && operateType != "del" {
        w.WriteHeader(400)
        w.Write([]byte("invalid operate_type\n"))
        return
    }
    circleNum, err := strconv.Atoi(req.FormValue("circle_num"))
    if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
        w.WriteHeader(400)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    db := strings.Trim(req.FormValue("db"), ",")
    var dbs []string
    if db != "" {
        dbs = strings.Split(db, ",")
    }

    cpus, err := strconv.Atoi(req.FormValue("cpus"))
    if err != nil || cpus <= 0 {
        w.WriteHeader(400)
        w.Write([]byte("invalid cpus\n"))
        return
    }
    hs.MigrateMaxCpus = cpus

    var backends []*backend.Backend
    if operateType == "del" {
        backendUrls := strings.Split(strings.Trim(req.FormValue("backends"), ","), ",")
        if len(backendUrls) == 0 {
            w.WriteHeader(400)
            w.Write([]byte("invalid backends\n"))
            return
        }
        for _, url := range backendUrls {
            backends = append(backends, &backend.Backend{
                Url: url,
                Client: backend.NewClient(url),
                Transport: backend.NewTransport(url)},
            )
            hs.BackendRebalanceStatus[circleNum][url] = &backend.MigrationInfo{}
            hs.Circles[circleNum].BackendWgMap[url] = &sync.WaitGroup{}
        }
    }
    for _, backend := range hs.Circles[circleNum].Backends {
        backends = append(backends, backend)
    }

    if hs.Circles[circleNum].IsMigrating {
        w.WriteHeader(202)
        w.Write([]byte(fmt.Sprintf("circle %d is migrating\n", circleNum)))
        return
    }
    if hs.IsResyncing {
        w.WriteHeader(202)
        w.Write([]byte("proxy is resyncing\n"))
        return
    }

    go hs.Rebalance(backends, circleNum, dbs)
    w.WriteHeader(202)
    w.Write([]byte("accepted\n"))
    return
}

func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    fromCircleNum, err := strconv.Atoi(req.FormValue("from_circle_num"))
    if err != nil {
        w.WriteHeader(400)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    toCircleNum, err := strconv.Atoi(req.FormValue("to_circle_num"))
    if err != nil {
        w.WriteHeader(400)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    if fromCircleNum < 0 || fromCircleNum >= len(hs.Circles) || toCircleNum < 0 || toCircleNum >= len(hs.Circles) || fromCircleNum == toCircleNum {
        w.WriteHeader(400)
        w.Write([]byte("invalid circle_num\n"))
        return
    }

    db := strings.Trim(req.FormValue("db"), ",")
    var dbs []string
    if db != "" {
        dbs = strings.Split(db, ",")
    }

    cpus, err := strconv.Atoi(req.FormValue("cpus"))
    if err != nil || cpus <= 0 {
        w.WriteHeader(400)
        w.Write([]byte("invalid cpus\n"))
        return
    }
    hs.MigrateMaxCpus = cpus

    if hs.Circles[fromCircleNum].IsMigrating || hs.Circles[toCircleNum].IsMigrating {
        w.WriteHeader(202)
        w.Write([]byte(fmt.Sprintf("circle %d or %d is migrating\n", fromCircleNum, toCircleNum)))
        return
    }
    if hs.IsResyncing {
        w.WriteHeader(202)
        w.Write([]byte("proxy is resyncing\n"))
        return
    }

    backendUrls := strings.Split(strings.Trim(req.FormValue("to_backends"), ","), ",")
    if len(backendUrls) == 0 {
        w.WriteHeader(400)
        w.Write([]byte("invalid backends\n"))
        return
    }

    go hs.Recovery(fromCircleNum, toCircleNum, backendUrls, dbs)
    w.WriteHeader(202)
    w.Write([]byte("accepted\n"))
    return
}

func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodPost {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    db := strings.Trim(req.FormValue("db"), ",")
    var dbs []string
    if db != "" {
        dbs = strings.Split(db, ",")
    }

    cpus, err := strconv.Atoi(req.FormValue("cpus"))
    if err != nil || cpus <= 0 {
        w.WriteHeader(400)
        w.Write([]byte("invalid cpus\n"))
        return
    }
    hs.MigrateMaxCpus = cpus

    lastSecondsStr := req.FormValue("last_seconds")
    if lastSecondsStr == "" {
        lastSecondsStr = "0"
    }
    lastSeconds, err := strconv.Atoi(lastSecondsStr)
    if err != nil || lastSeconds < 0 {
        w.WriteHeader(400)
        w.Write([]byte("invalid latest_seconds\n"))
        return
    }

    for _, circle := range hs.Circles {
        if circle.IsMigrating {
            w.WriteHeader(202)
            w.Write([]byte(fmt.Sprintf("circle %d is migrating\n", circle.CircleNum)))
            return
        }
    }
    if hs.IsResyncing {
        w.WriteHeader(202)
        w.Write([]byte("proxy is resyncing\n"))
        return
    }

    go hs.Resync(dbs, lastSeconds)
    w.WriteHeader(202)
    w.Write([]byte("accepted\n"))
    return
}

func (hs *HttpService) HandlerStatus(w http.ResponseWriter, req *http.Request) {
    defer req.Body.Close()
    hs.AddHeader(w)

    if req.Method != http.MethodGet {
        w.WriteHeader(405)
        w.Write(util.StatusText(405))
        return
    }

    circleNumStr := req.FormValue("circle_num")
    circleNum, err := strconv.Atoi(circleNumStr)
    if err != nil || circleNum < 0 || circleNum >= len(hs.Circles) {
        w.WriteHeader(400)
        w.Write([]byte("invalid circle_num\n"))
        return
    }
    var res []byte
    statusType := req.FormValue("type")
    if statusType == "rebalance" {
        res, _ = json.Marshal(hs.BackendRebalanceStatus[circleNum])
    } else if statusType == "recovery" {
        res, _ = json.Marshal(hs.BackendRecoveryStatus[circleNum])
    } else if statusType == "resync" {
        res, _ = json.Marshal(hs.BackendResyncStatus[circleNum])
    } else {
        w.WriteHeader(400)
        w.Write([]byte("invalid status type\n"))
        return
    }

    w.Write(res)
    return
}

func (hs *HttpService) AddHeader(w http.ResponseWriter) {
    w.Header().Add("X-Influxdb-Version", util.Version)
}

func (hs *HttpService) transAuth(ctx string) string {
    if hs.AuthSecure {
        return util.AesEncrypt(ctx, util.CipherKey)
    } else {
        return ctx
    }
}

func (hs *HttpService) checkAuth(r *http.Request) bool {
    if hs.Username == "" && hs.Password == "" {
        return true
    }
    u, p := r.URL.Query().Get("u"), r.URL.Query().Get("p")
    if hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password  {
        return true
    }
    u, p, ok := r.BasicAuth()
    if ok && hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
        return true
    }
    return false
}

func (hs *HttpService) checkDatabase(q string) bool {
    q = strings.ToLower(q)
    return (strings.HasPrefix(q, "show") && strings.Contains(q, "databases")) || (strings.HasPrefix(q, "create") && strings.Contains(q, "database"))
}
