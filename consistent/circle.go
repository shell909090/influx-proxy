package consistent

import (
    "fmt"
    "github.com/chengshiwen/influx-proxy/util"
    "github.com/influxdata/influxdb1-client/models"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "sync"
    "time"
)

type Circle struct {
    Name           string                     `json:"name"`
    Router         *util.Consistent           `json:"router"`
    Backends       []*Backend                 `json:"backends"`
    UrlToBackend   map[string]*Backend        `json:"url_to_backend"`
    BackendWgMap   map[string]*sync.WaitGroup `json:"backend_wg_map"`
    CircleNum      int                        `json:"circle_num"`
    ReadyMigrating bool                       `json:"ready_migrating"` // bool: 准备迁移标志，false: 取消准备迁移标志
    IsMigrating    bool                       `json:"is_migrating"`    // true: 正在迁移 false: 未在迁移
    WgMigrate      *sync.WaitGroup            `json:"wg_migrate"`
    StatusLock     *sync.RWMutex              `json:"status_lock"`
}

type MigrateFlagStatus struct {
    ReadyMigratingFlag bool `json:"ready_migrating_flag"`
    IsMigratingFlag    bool `json:"is_migrating_flag"`
}

func (circle *Circle) CheckStatus() bool {
    res := true
    for _, backend := range circle.Backends {
        res = res && backend.Active
    }
    return res
}

// 执行 show 查询操作
func (circle *Circle) QueryCluster(req *http.Request, backends []*Backend) ([]byte, error) {
    bodies := make([][]byte, 0)
    // 在环内的所有数据库实例上执行查询，再聚合在一起
    for _, backend := range backends {
        body, err := backend.Query(req)
        if err != nil {
            util.Log.Errorf("req:%+v err:%+v", req, err)
            return nil, err
        }
        if body != nil {
            bodies = append(bodies, body)
        }
    }

    // 针对集群语句特征执行不同的聚合过程
    q := strings.ToLower(strings.TrimSpace(req.FormValue("q")))
    if strings.HasPrefix(q, "show") {
        if strings.Contains(q, "databases") || strings.Contains(q, "series") || strings.Contains(q, "measurements") {
            return circle.reduceByValues(bodies)
        } else if strings.Contains(q, "keys") {
            return circle.reduceBySeries(bodies)
        }
    }
    return nil, nil
}

func (circle *Circle) reduceByValues(bodies [][]byte) (rBody []byte, err error) {
    valuesMap := make(map[string]*models.Row)
    for _, body := range bodies {
        _series, _err := SeriesFromResponseBytes(body)
        if _err != nil {
            util.Log.Errorf("err:%+v", _err)
            err = _err
            return
        }
        for _, s := range _series {
            for _, value := range s.Values {
                if len(value) < 1 {
                    util.Log.Errorf("value length:%+v value:%+v", len(value), value)
                    continue
                }
                key := value[0].(string)
                valuesMap[key] = s
            }
        }
    }
    serie := &models.Row{}
    var values [][]interface{}
    for v, s := range valuesMap {
        values = append(values, []interface{}{v})
        serie = s
    }
    serie.Values = values
    if len(values) > 0 {
        rBody, err = ResponseBytesFromSeries(models.Rows{serie})
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            return
        }
    } else {
        rBody, _ = ResponseBytesFromSeries(nil)
    }
    return
}

func (circle *Circle) reduceBySeries(bodies [][]byte) (rBody []byte, err error) {
    seriesMap := make(map[string]*models.Row)
    for _, body := range bodies {
        _series, _err := SeriesFromResponseBytes(body)
        if _err != nil {
            util.Log.Errorf("err:%+v", _err)
            err = _err
            return
        }
        for _, s := range _series {
            seriesMap[s.Name] = s
        }
    }

    var series models.Rows
    for _, item := range seriesMap {
        series = append(series, item)
    }
    rBody, err = ResponseBytesFromSeries(series)
    if err != nil {
        util.Log.Errorf("err:%+v", err)
    }
    return
}

func (circle *Circle) Query(req *http.Request) ([]byte, error) {
    // 得到key
    q := req.FormValue("q")
    measurement, e := GetMeasurementFromInfluxQL(q)
    if e != nil {
        return nil, e
    }
    db := req.FormValue("db")
    key := db + "," + measurement

    // 得到目标数据库
    backendUrl, e := circle.Router.Get(key)
    if e != nil {
        return nil, e
    }
    backend := circle.UrlToBackend[backendUrl]
    return backend.Query(req)
}

func (circle *Circle) GetSeriesValues(req *http.Request, backends []*Backend) []string {
    p, _ := circle.QueryCluster(req, backends)
    res, _ := SeriesFromResponseBytes(p)
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

func (circle *Circle) Migrate(srcBackend *Backend, dstBackends []*Backend, db, measure string, lastSeconds int) error {
    timeLimitStr := ""
    if lastSeconds > 0 {
        timeLimitStr = fmt.Sprintf(" where time >= %ds", time.Now().Unix()-int64(lastSeconds))
    }

    dataReq := &http.Request{
        Form:   url.Values{"q": []string{fmt.Sprintf("select * from \"%s\"%s", measure, timeLimitStr)}, "db": []string{db}},
        Header: http.Header{"User-Agent": []string{"curl/7.54.0"}, "Accept": []string{"*/*"}},
    }
    res, err := srcBackend.Query(dataReq)
    if err != nil {
        return err
    }

    series, err := SeriesFromResponseBytes(res)
    if err != nil {
        return err
    }
    if len(series) < 1 {
        return nil
    }
    columns := series[0].Columns

    tagReq := &http.Request{
        Form:   url.Values{"q": []string{fmt.Sprintf("show tag keys from \"%s\"%s", measure, timeLimitStr)}, "db": []string{db}},
        Header: http.Header{"User-Agent": []string{"curl/7.54.0"}, "Accept": []string{"*/*"}},
    }
    tags := circle.GetSeriesValues(tagReq, []*Backend{srcBackend})

    fieldReq := &http.Request{
        Form:   url.Values{"q": []string{fmt.Sprintf("show field keys from \"%s\"%s", measure, timeLimitStr)}, "db": []string{db}},
        Header: http.Header{"User-Agent": []string{"curl/7.54.0"}, "Accept": []string{"*/*"}},
    }
    fields := circle.GetSeriesValues(fieldReq, []*Backend{srcBackend})

    var lines []string
    for key, value := range series[0].Values {
        columnToValue := make(map[string]string)
        if key % 20000 == 0 {
            if len(lines) != 0 {
                lineData := strings.Join(lines, "\n")
                for _, dstBackend := range dstBackends {
                    err = dstBackend.Write(db, []byte(lineData), true)
                    if err != nil {
                        return err
                    }
                }
            }
        }

        for k, v := range value {
            var vStr string
            switch  v.(type) {
            case float64:
                vStr = strconv.FormatFloat(v.(float64), 'f', -1, 64)
            case bool:
                if v.(bool) {
                    vStr = "true"
                } else {
                    vStr = "false"
                }
            case string:
                vStr = v.(string)
            }
            columnToValue[columns[k]] = columns[k] + "=" + vStr
        }
        tagStr := []string{measure}
        for _, v := range tags {
            tagStr = append(tagStr, columnToValue[v])
        }
        line1 := strings.Join(tagStr, ",")

        fieldStr := make([]string, 0)
        for _, v := range fields {
            fieldStr = append(fieldStr, columnToValue[v])
        }

        line2 := strings.Join(fieldStr, ",")

        timeStr := strings.Split(columnToValue["time"], "=")[1]

        ts, _ := time.Parse(time.RFC3339Nano, timeStr)

        line3 := strconv.FormatInt(ts.UnixNano(), 10)

        line := strings.Join([]string{line1, line2, line3}, " ")
        lines = append(lines, line)
    }

    lineData := strings.Join(lines, "\n")
    for _, dstBackend := range dstBackends {
        err = dstBackend.Write(db, []byte(lineData), true)
        if err != nil {
            return err
        }
    }
    return nil
}

func (circle *Circle) GetIsMigrating() bool {
    circle.StatusLock.RLock()
    defer circle.StatusLock.RUnlock()
    return circle.IsMigrating
}

func (circle *Circle) SetIsMigrating(b bool) {
    circle.StatusLock.Lock()
    defer circle.StatusLock.Unlock()
    circle.IsMigrating = b
}

func (circle *Circle) GetReadyMigrating() bool {
    circle.StatusLock.RLock()
    defer circle.StatusLock.RUnlock()
    return circle.ReadyMigrating
}

func (circle *Circle) SetReadyMigrating(b bool) {
    circle.StatusLock.Lock()
    defer circle.StatusLock.Unlock()
    circle.ReadyMigrating = b
}
