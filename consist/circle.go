package consist

import (
    "github.com/chengshiwen/influx-proxy/util"
    "net/http"
    "strings"
    "sync"
)

type CirCle struct {
    Router         *Consistent                `json:"router"`     // 一般情况下使用 NewRouter
    BackendS       []*Backend                 `json:"backends"`
    UrlToMap       map[string]*Backend        `json:"url_to_map"`
    BackendWgMap   map[string]*sync.WaitGroup `json:"backend_wg_map"`
    CircleNum      int                        `json:"circle_num"`
}

func (circle *CirCle) CheckStatus() bool {
    res := true
    for _, backend := range circle.BackendS {
        res = res && backend.Active
    }
    return res
}

// 执行  show 查询操作
func (circle *CirCle) QueryShow(req *http.Request, backendS []*Backend) ([]byte, error) {
    res := make([][]byte, 0)
    // 在环内的所有数据库实例上执行show，在聚合在一起
    for _, backend := range backendS {
        body, err := backend.QueryShow(req)
        if err != nil {
            util.CustomLog.Errorf("req:%+v err:%+v", req, err)
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

func (circle *CirCle) showMeasurements(bodys [][]byte) (fBody []byte, err error) {
    measureMap := make(map[string]*seri)
    
    for _, body := range bodys {
        sSs, Err := GetSeriesArray(body)
        if Err != nil {
            util.CustomLog.Errorf("err:%+v", Err)
            err = Err
            return
        }
        for _, s := range sSs {
            for _, measurement := range s.Values {
                if len(measurement) < 1 {
                    util.CustomLog.Errorf("length of measurement:%+v measurement:%+v", len(measurement), measurement)
                    continue
                }
                measure := measurement[0].(string)
                if strings.Contains(measure, "influxdb.cluster") {
                    continue
                }
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
        util.CustomLog.Errorf("err:%+v", err)
        return
    }
    return
    
}

func (circle *CirCle) showTagFieldkey(bodys [][]byte) (fBody []byte, err error) {
    seriesMap := make(map[string]*seri)
    for _, body := range bodys {
        sSs, Err := GetSeriesArray(body)
        if Err != nil {
            util.CustomLog.Errorf("err:%+v", Err)
            err = Err
            return
        }
        for _, s := range sSs {
            if strings.Contains(s.Name, "influxdb.cluster") {
                continue
            }
            seriesMap[s.Name] = s
        }
    }
    
    var series []*seri
    for _, item := range seriesMap {
        series = append(series, item)
    }
    fBody, err = GetJsonBodyfromSeries(series)
    if err != nil {
        util.CustomLog.Errorf("err:%+v", err)
    }
    return
    
}

func (circle *CirCle) Query(req *http.Request) ([]byte, error) {
    // 得到dbMeasure
    q := req.FormValue("q")
    measure, e := GetMeasurementFromInfluxQL(q)
    if e != nil {
        return nil, e
    }
    db := req.FormValue("db")
    dbMeasure := db + "," + measure
    
    // 得到目标数据库
    backUrl, e := circle.Router.Get(dbMeasure)
    if e != nil {
        return nil, e
    }
    backend := circle.UrlToMap[backUrl]
    return backend.QueryShow(req)
}
