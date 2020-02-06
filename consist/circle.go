package consist

import (
    "github.com/chengshiwen/influx-proxy/util"
    "net/http"
    "strings"
    "sync"
)

type Circle struct {
    Router         *Consistent                `json:"router"`
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
func (circle *Circle) QueryShow(req *http.Request, backends []*Backend) ([]byte, error) {
    res := make([][]byte, 0)
    // 在环内的所有数据库实例上执行show，在聚合在一起
    for _, backend := range backends {
        body, err := backend.Query(req)
        if err != nil {
            util.Log.Errorf("req:%+v err:%+v", req, err)
            return nil, err
        }
        if body != nil {
            res = append(res, body)
        }
    }
    q := req.Form.Get("q")

    // 针对measurements 和 databases 执行不同的聚合过程
    if strings.Contains(q, "measurements") || strings.Contains(q, "databases") {
        return circle.showMeasurements(res)
    } else {
        return circle.showTagFieldkey(res)
    }
}

func (circle *Circle) showMeasurements(bodies [][]byte) (fBody []byte, err error) {
    measureMap := make(map[string]*seri)
    for _, body := range bodies {
        sSs, Err := GetSeriesArray(body)
        if Err != nil {
            util.Log.Errorf("err:%+v", Err)
            err = Err
            return
        }
        for _, s := range sSs {
            for _, measurement := range s.Values {
                if len(measurement) < 1 {
                    util.Log.Errorf("length of measurement:%+v measurement:%+v", len(measurement), measurement)
                    continue
                }
                measure := measurement[0].(string)
                measureMap[measure] = s
            }
        }
    }
    serie := &seri{}
    var measures [][]interface{}
    for measure, s := range measureMap {
        measures = append(measures, []interface{}{measure})
        serie = s
    }
    serie.Values = measures
    fBody, err = GetJsonBodyfromSeries([]*seri{serie})
    if err != nil {
        util.Log.Errorf("err:%+v", err)
        return
    }
    return
}

func (circle *Circle) showTagFieldkey(bodies [][]byte) (fBody []byte, err error) {
    seriesMap := make(map[string]*seri)
    for _, body := range bodies {
        sSs, Err := GetSeriesArray(body)
        if Err != nil {
            util.Log.Errorf("err:%+v", Err)
            err = Err
            return
        }
        for _, s := range sSs {
            seriesMap[s.Name] = s
        }
    }

    var series []*seri
    for _, item := range seriesMap {
        series = append(series, item)
    }
    fBody, err = GetJsonBodyfromSeries(series)
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
