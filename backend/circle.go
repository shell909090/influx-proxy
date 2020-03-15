package backend

import (
    "bytes"
    "fmt"
    "github.com/chengshiwen/influx-proxy/util"
    "github.com/influxdata/influxdb1-client/models"
    "io/ioutil"
    "log"
    "net/http"
    "stathat.com/c/consistent"
    "strings"
    "sync"
    "time"
)

type Circle struct {
    Name            string                      `json:"name"`
    Router          *consistent.Consistent      `json:"router"`
    Backends        []*Backend                  `json:"backends"`
    UrlToBackend    map[string]*Backend         `json:"url_to_backend"`
    BackendWgMap    map[string]*sync.WaitGroup  `json:"backend_wg_map"`
    CircleId        int                         `json:"circle_id"`
    IsMigrating     bool                        `json:"is_migrating"`
    WgMigrate       *sync.WaitGroup             `json:"wg_migrate"`
    StatusLock      *sync.RWMutex               `json:"status_lock"`
}

func (circle *Circle) CheckStatus() bool {
    for _, backend := range circle.Backends {
        if !backend.Active {
            return false
        }
    }
    return true
}

func (circle *Circle) Query(w http.ResponseWriter, req *http.Request) ([]byte, error) {
    q := req.FormValue("q")
    measurement, e := GetMeasurementFromInfluxQL(q)
    if e != nil {
        return nil, e
    }
    db := req.FormValue("db")
    key := GetKey(db, measurement)
    backendUrl, e := circle.Router.Get(key)
    if e != nil {
        return nil, e
    }
    backend := circle.UrlToBackend[backendUrl]
    // fmt.Printf("%s key: %s; backend: %s %s; query: %s\n", time.Now().Format("2006-01-02 15:04:05"), key, backend.Name, backend.Url, q)
    return backend.Query(req, w, false)
}

func (circle *Circle) QueryCluster(w http.ResponseWriter, req *http.Request) ([]byte, error) {
    // remove support of query parameter `chunked`
    req.Form.Del("chunked")
    var reqBodyBytes []byte
    if req.Body != nil {
        reqBodyBytes, _ = ioutil.ReadAll(req.Body)
    }
    bodies := make([][]byte, 0)

    for _, backend := range circle.Backends {
        req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
        body, err := backend.Query(req, w, true)
        if err != nil {
            log.Printf("backend query error: %s %s", backend.Url, err)
            return nil, err
        }
        if body != nil {
            bodies = append(bodies, body)
        }
    }

    var body []byte
    var err error
    q := strings.ToLower(strings.TrimSpace(req.FormValue("q")))
    // fmt.Printf("%s circle: %s; query: %s\n", time.Now().Format("2006-01-02 15:04:05"), circle.Name, q)
    if strings.HasPrefix(q, "show") {
        if strings.Contains(q, "measurements") || strings.Contains(q, "series") || strings.Contains(q, "databases") {
            body, err = circle.reduceByValues(bodies)
        } else if (strings.Contains(q, "field") || strings.Contains(q, "tag")) && (strings.Contains(q, "keys") || strings.Contains(q, "values")) {
            body, err = circle.reduceBySeries(bodies)
        } else if strings.Contains(q, "stats") {
            body, err = circle.concatByResults(bodies)
        } else if strings.Contains(q, "retention") && strings.Contains(q, "policies") {
            body, err = circle.concatByValues(bodies)
        }
    }
    if err != nil {
        return nil, err
    }
    if body == nil {
        body, _ = ResponseBytesFromSeries(nil)
    }
    if w.Header().Get("Content-Encoding") == "gzip" {
        return util.GzipCompress(body)
    } else {
        return body, nil
    }
}

func (circle *Circle) reduceByValues(bodies [][]byte) (body []byte, err error) {
    valuesMap := make(map[string]*models.Row)
    for _, b := range bodies {
        _series, _err := SeriesFromResponseBytes(b)
        if _err != nil {
            err = _err
            return
        }
        for _, s := range _series {
            for _, value := range s.Values {
                if len(value) < 1 {
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
    if len(values) > 0 {
        serie.Values = values
        body, err = ResponseBytesFromSeries(models.Rows{serie})
    } else {
        body, _ = ResponseBytesFromSeries(nil)
    }
    return
}

func (circle *Circle) reduceBySeries(bodies [][]byte) (body []byte, err error) {
    seriesMap := make(map[string]*models.Row)
    for _, b := range bodies {
        _series, _err := SeriesFromResponseBytes(b)
        if _err != nil {
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
    body, err = ResponseBytesFromSeries(series)
    return
}

func (circle *Circle) concatByResults(bodies [][]byte) (body []byte, err error) {
    var results []*Result
    for _, b := range bodies {
        _results, _err := ResultsFromResponseBytes(b)
        if _err != nil {
            err = _err
            return
        }
        if len(_results) == 1 {
            results = append(results, _results[0])
        }
    }
    body, err = ResponseBytesFromResults(results)
    return
}

func (circle *Circle) concatByValues(bodies [][]byte) (body []byte, err error) {
    var series []*models.Row
    var values [][]interface{}
    for _, b := range bodies {
        _series, _err := SeriesFromResponseBytes(b)
        if _err != nil {
            err = _err
            return
        }
        if len(_series) == 1 {
            series = _series
            for _, value := range _series[0].Values {
                values = append(values, value)
            }
        }
    }
    if len(series) == 1 {
        series[0].Values = values
    }
    body, err = ResponseBytesFromSeries(series)
    return
}

func (circle *Circle) Migrate(srcBackend *Backend, dstBackends []*Backend, db, measure string, seconds int) error {
    timeClause := ""
    if seconds > 0 {
        timeClause = fmt.Sprintf(" where time >= %ds", time.Now().Unix()-int64(seconds))
    }

    rsp, err := srcBackend.QueryIQL(db, fmt.Sprintf("select * from \"%s\"%s", measure, timeClause))
    if err != nil {
        return err
    }
    series, err := SeriesFromResponseBytes(rsp)
    if err != nil {
        return err
    }
    if len(series) < 1 {
        return nil
    }
    columns := series[0].Columns

    tagKeys := srcBackend.GetTagKeys(db, measure)
    tagMap := make(map[string]bool, 0)
    for _, t := range tagKeys {
        tagMap[t] = true
    }
    fieldKeys := srcBackend.GetFieldKeys(db, measure)

    vlen := len(series[0].Values)
    var lines []string
    for idx, value := range series[0].Values {
        mtagSet := []string{util.EscapeMeasurement(measure)}
        fieldSet := make([]string, 0)
        for i := 1; i < len(value); i++ {
            k := columns[i]
            v := value[i]
            if _, ok := tagMap[k]; ok {
                if v != nil {
                    mtagSet = append(mtagSet, fmt.Sprintf("%s=%s", util.EscapeTag(k), util.EscapeTag(v.(string))))
                }
            } else if vtype, ok := fieldKeys[k]; ok {
                if v != nil {
                    if vtype == "float" || vtype == "boolean" {
                        fieldSet = append(fieldSet, fmt.Sprintf("%s=%v", util.EscapeTag(k), v))
                    } else if vtype == "integer" {
                        fieldSet = append(fieldSet, fmt.Sprintf("%s=%di", util.EscapeTag(k), int64(v.(float64))))
                    } else if vtype == "string" {
                        fieldSet = append(fieldSet, fmt.Sprintf("%s=\"%s\"", util.EscapeTag(k), models.EscapeStringField(v.(string))))
                    }
                }
            }
        }
        mtagStr := strings.Join(mtagSet, ",")
        fieldStr := strings.Join(fieldSet, ",")
        ts, _ := time.Parse(time.RFC3339Nano, value[0].(string))
        line := fmt.Sprintf("%s %s %d", mtagStr, fieldStr, ts.UnixNano())
        lines = append(lines, line)
        if (idx + 1) % util.MigrateBatchSize == 0 || idx + 1 == vlen {
            if len(lines) != 0 {
                lineData := strings.Join(lines, "\n")
                for _, dstBackend := range dstBackends {
                    err = dstBackend.WriteCompressed(db, []byte(lineData))
                    if err != nil {
                        return err
                    }
                }
                lines = lines[:0]
            }
        }
    }
    return nil
}

func (circle *Circle) SetMigrating(migrating bool) {
    circle.StatusLock.Lock()
    defer circle.StatusLock.Unlock()
    circle.IsMigrating = migrating
}
