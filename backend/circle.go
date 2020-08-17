package backend

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strconv"

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
	routerCaches map[string]*Backend
	mapToBackend map[string]*Backend
}

func NewCircle(cfg *CircleConfig, pxcfg *ProxyConfig, circleId int) (ic *Circle) { // nolint:golint
	ic = &Circle{
		CircleId:     circleId,
		Name:         cfg.Name,
		Backends:     make([]*Backend, len(cfg.Backends)),
		WriteOnly:    false,
		router:       consistent.New(),
		routerCaches: make(map[string]*Backend),
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
		ic.router.Add(be.Url)
		ic.mapToBackend[be.Url] = be
	} else {
		// each additional backend causes 10% hash collision from 11th backend
		ic.router.Add(strconv.Itoa(idx))
		ic.mapToBackend[strconv.Itoa(idx)] = be
	}
}

func (ic *Circle) GetBackend(key string) *Backend {
	if be, ok := ic.routerCaches[key]; ok {
		return be
	}
	value, _ := ic.router.Get(key)
	be := ic.mapToBackend[value]
	ic.routerCaches[key] = be
	return be
}

func (ic *Circle) GetHealth() []map[string]interface{} {
	stats := make([]map[string]interface{}, len(ic.Backends))
	for i, be := range ic.Backends {
		stats[i] = be.GetHealth(ic)
	}
	return stats
}

func (ic *Circle) CheckActive() bool {
	for _, be := range ic.Backends {
		if !be.Active {
			return false
		}
	}
	return true
}

func (ic *Circle) Query(w http.ResponseWriter, req *http.Request, tokens []string) ([]byte, error) {
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	var reqBodyBytes []byte
	if req.Body != nil {
		reqBodyBytes, _ = ioutil.ReadAll(req.Body)
	}
	bodies := make([][]byte, 0)

	for _, be := range ic.Backends {
		req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
		body, err := be.Query(req, w, true)
		if err != nil {
			return nil, err
		}
		if body != nil {
			bodies = append(bodies, body)
		}
	}

	var rsp *Response
	var err error
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
		return nil, err
	}
	if rsp == nil {
		rsp = ResponseFromSeries(nil)
	}
	pretty := req.FormValue("pretty") == "true"
	body := util.MarshalJSON(rsp, pretty)
	if w.Header().Get("Content-Encoding") == "gzip" {
		return util.GzipCompress(body)
	}
	return body, nil
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
