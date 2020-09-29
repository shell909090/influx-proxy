package backend

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/chengshiwen/influx-proxy/util"
	"github.com/influxdata/influxdb1-client/models"
	"stathat.com/c/consistent"
)

type Circle struct {
	CircleId     int // nolint:golint
	Name         string
	Backends     []*Backend
	WriteOnly    bool
	router       *consistent.Consistent
	routerCaches sync.Map
	mapToBackend map[string]*Backend
}

func NewCircle(cfg *CircleConfig, pxcfg *ProxyConfig, circleId int) (ic *Circle) { // nolint:golint
	ic = &Circle{
		CircleId:     circleId,
		Name:         cfg.Name,
		Backends:     make([]*Backend, len(cfg.Backends)),
		WriteOnly:    false,
		router:       consistent.New(),
		mapToBackend: make(map[string]*Backend),
	}
	ic.router.NumberOfReplicas = 256
	for idx, bkcfg := range cfg.Backends {
		ic.Backends[idx] = NewBackend(bkcfg, pxcfg)
		ic.addRouter(ic.Backends[idx], idx, pxcfg.HashKey)
	}
	return
}

func (ic *Circle) addRouter(be *Backend, idx int, hashKey string) {
	if hashKey == "name" {
		ic.router.Add(be.Name)
		ic.mapToBackend[be.Name] = be
	} else if hashKey == "url" {
		// compatible with version <= 2.3
		ic.router.Add(be.Url)
		ic.mapToBackend[be.Url] = be
	} else if hashKey == "exi" {
		// exi: extended index, recommended, started with 2.5+
		// no hash collision will occur before idx <= 100000, which has been tested
		str := "|" + strconv.Itoa(idx)
		ic.router.Add(str)
		ic.mapToBackend[str] = be
	} else {
		// idx: default index, compatible with version 2.4, recommended when the number of backends <= 10
		// each additional backend causes 10% hash collision from 11th backend
		str := strconv.Itoa(idx)
		ic.router.Add(str)
		ic.mapToBackend[str] = be
	}
}

func (ic *Circle) GetBackend(key string) *Backend {
	if be, ok := ic.routerCaches.Load(key); ok {
		return be.(*Backend)
	}
	value, _ := ic.router.Get(key)
	be := ic.mapToBackend[value]
	ic.routerCaches.Store(key, be)
	return be
}

func (ic *Circle) GetHealth() interface{} {
	var wg sync.WaitGroup
	backends := make([]interface{}, len(ic.Backends))
	for i, be := range ic.Backends {
		wg.Add(1)
		go func(i int, be *Backend) {
			defer wg.Done()
			backends[i] = be.GetHealth(ic)
		}(i, be)
	}
	wg.Wait()
	circle := struct {
		Id        int    `json:"id"` // nolint:golint
		Name      string `json:"name"`
		Active    bool   `json:"active"`
		WriteOnly bool   `json:"write_only"`
	}{ic.CircleId, ic.Name, ic.IsActive(), ic.WriteOnly}
	health := struct {
		Circle   interface{} `json:"circle"`
		Backends interface{} `json:"backends"`
	}{circle, backends}
	return health
}

func (ic *Circle) GetActiveCount() int {
	count := 0
	for _, be := range ic.Backends {
		if be.Active {
			count++
		}
	}
	return count
}

func (ic *Circle) IsActive() bool {
	for _, be := range ic.Backends {
		if !be.Active {
			return false
		}
	}
	return true
}

func (ic *Circle) Query(w http.ResponseWriter, req *http.Request, tokens []string) (body []byte, err error) {
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	bodies, inactive, err := QueryInParallel(ic.Backends, req, w, true)
	if err != nil {
		return
	}
	if inactive > 0 && len(bodies) == 0 {
		return nil, ErrBackendsUnavailable
	}

	var rsp *Response
	stmt2 := GetHeadStmtFromTokens(tokens, 2)
	stmt3 := GetHeadStmtFromTokens(tokens, 3)
	if stmt2 == "show measurements" || stmt2 == "show series" || stmt2 == "show databases" {
		rsp, err = ic.reduceByValues(bodies)
	} else if stmt3 == "show field keys" || stmt3 == "show tag keys" || stmt3 == "show tag values" {
		rsp, err = ic.reduceBySeries(bodies)
	} else if stmt3 == "show retention policies" {
		rsp, err = ic.concatByValues(bodies)
	} else if stmt2 == "show stats" {
		rsp, err = ic.concatByResults(bodies)
	}
	if err != nil {
		return
	}
	if rsp == nil {
		rsp = ResponseFromSeries(nil)
	}
	if inactive > 0 {
		rsp.Err = fmt.Sprintf("%d/%d backends unavailable", inactive, inactive+len(bodies))
	}
	pretty := req.URL.Query().Get("pretty") == "true"
	body = util.MarshalJSON(rsp, pretty)
	if w.Header().Get("Content-Encoding") == "gzip" {
		return util.GzipCompress(body)
	}
	return
}

func (ic *Circle) reduceByValues(bodies [][]byte) (rsp *Response, err error) {
	var series models.Rows
	var values [][]interface{}
	valuesMap := make(map[string][]interface{})
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		if len(_series) == 1 {
			series = _series
			for _, value := range _series[0].Values {
				key := value[0].(string)
				valuesMap[key] = value
			}
		}
	}
	if len(series) == 1 {
		for _, value := range valuesMap {
			values = append(values, value)
		}
		if len(values) > 0 {
			series[0].Values = values
		} else {
			series = nil
		}
	}
	return ResponseFromSeries(series), nil
}

func (ic *Circle) reduceBySeries(bodies [][]byte) (rsp *Response, err error) {
	var series models.Rows
	seriesMap := make(map[string]*models.Row)
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		for _, s := range _series {
			seriesMap[s.Name] = s
		}
	}
	for _, item := range seriesMap {
		series = append(series, item)
	}
	return ResponseFromSeries(series), nil
}

func (ic *Circle) concatByValues(bodies [][]byte) (rsp *Response, err error) {
	var series []*models.Row
	var values [][]interface{}
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		if len(_series) == 1 {
			series = _series
			values = append(values, _series[0].Values...)
		}
	}
	if len(series) == 1 {
		series[0].Values = values
	}
	return ResponseFromSeries(series), nil
}

func (ic *Circle) concatByResults(bodies [][]byte) (rsp *Response, err error) {
	var results []*Result
	for _, b := range bodies {
		_results, err := ResultsFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		if len(_results) == 1 {
			results = append(results, _results[0])
		}
	}
	return ResponseFromResults(results), nil
}
