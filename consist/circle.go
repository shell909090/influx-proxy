package consist

import (
    "github.com/chengshiwen/influx-proxy/util"
    "net/http"
    "strings"
    "sync"
)

type Circle struct {
    Router         *util.Consistent           `json:"router"`
    Backends       []*Backend                 `json:"backends"`
    UrlToMap       map[string]*Backend        `json:"url_to_map"`
    BackendWgMap   map[string]*sync.WaitGroup `json:"backend_wg_map"`
    CircleNum      int                        `json:"circle_num"`
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
    valuesMap := make(map[string]*seri)
    for _, body := range bodies {
        _series, _err := GetSeriesArray(body)
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
    serie := &seri{}
    var values [][]interface{}
    for v, s := range valuesMap {
        values = append(values, []interface{}{v})
        serie = s
    }
    serie.Values = values
    if len(values) > 0 {
        rBody, err = GetJsonBodyfromSeries([]*seri{serie})
        if err != nil {
            util.Log.Errorf("err:%+v", err)
            return
        }
    } else {
        rBody, _ = GetJsonBodyfromSeries(nil)
    }
    return
}

func (circle *Circle) reduceBySeries(bodies [][]byte) (rBody []byte, err error) {
    seriesMap := make(map[string]*seri)
    for _, body := range bodies {
        _series, _err := GetSeriesArray(body)
        if _err != nil {
            util.Log.Errorf("err:%+v", _err)
            err = _err
            return
        }
        for _, s := range _series {
            seriesMap[s.Name] = s
        }
    }

    var series []*seri
    for _, item := range seriesMap {
        series = append(series, item)
    }
    rBody, err = GetJsonBodyfromSeries(series)
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
    backend := circle.UrlToMap[backendUrl]
    return backend.Query(req)
}
