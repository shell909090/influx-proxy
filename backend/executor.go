// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sync"

	"github.com/chengshiwen/influx-proxy/util"
	"github.com/influxdata/influxdb1-client/models"
)

var (
	ErrEmptyQuery          = errors.New("empty query")
	ErrDatabaseNotFound    = errors.New("database not found")
	ErrBackendsUnavailable = errors.New("backends unavailable")
	ErrGetMeasurement      = errors.New("can't get measurement")
	ErrGetBackends         = errors.New("can't get backends")
)

func QueryFromQL(w http.ResponseWriter, req *http.Request, ip *Proxy, tokens []string, db string) (body []byte, err error) {
	// available circle -> backend by key(db,meas) -> select or show
	meas, err := GetMeasurementFromTokens(tokens)
	if err != nil {
		return nil, ErrGetMeasurement
	}
	key := GetKey(db, meas)
	badSet := make(map[int]bool)
	for {
		if len(badSet) == len(ip.Circles) {
			return nil, ErrBackendsUnavailable
		}
		id := rand.Intn(len(ip.Circles))
		if badSet[id] {
			continue
		}
		circle := ip.Circles[id]
		if circle.WriteOnly {
			badSet[id] = true
			continue
		}
		be := circle.GetBackend(key)
		if be.IsActive() {
			qr := be.Query(req, w, false)
			if qr.Status > 0 || len(badSet) == len(ip.Circles)-1 {
				return qr.Body, qr.Err
			}
		}
		badSet[id] = true
	}
}

func QueryShowQL(w http.ResponseWriter, req *http.Request, ip *Proxy, tokens []string) (body []byte, err error) {
	// all circles -> all backends -> show
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	backends := make([]*Backend, 0)
	for _, circle := range ip.Circles {
		backends = append(backends, circle.Backends...)
	}
	bodies, inactive, err := QueryInParallel(backends, req, w, true)
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
		rsp, err = reduceByValues(bodies)
	} else if stmt3 == "show field keys" || stmt3 == "show tag keys" || stmt3 == "show tag values" {
		rsp, err = reduceBySeries(bodies)
	} else if stmt3 == "show retention policies" {
		rsp, err = concatByValues(bodies)
	} else if stmt2 == "show stats" {
		rsp, err = concatByResults(bodies)
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

/**
  for retention policy
*/
func QueryRetentionPolicyQL(w http.ResponseWriter, req *http.Request, ip *Proxy) (body []byte, err error) {
	// all circles -> all backends -> create or drop retention policy
	for _, circle := range ip.Circles {
		if !circle.IsActive() {
			return nil, fmt.Errorf("circle %d(%s) unavailable", circle.CircleId, circle.Name)
		}
	}
	backends := make([]*Backend, 0)
	for _, circle := range ip.Circles {
		backends = append(backends, circle.Backends...)
	}
	bodies, _, err := QueryInParallel(backends, req, w, false)
	if err != nil {
		return nil, err
	}
	return bodies[0], nil
}

func QueryDeleteOrDropQL(w http.ResponseWriter, req *http.Request, ip *Proxy, tokens []string, db string) (body []byte, err error) {
	// all circles -> backend by key(db,meas) -> delete or drop
	meas, err := GetMeasurementFromTokens(tokens)
	if err != nil {
		return nil, err
	}
	key := GetKey(db, meas)
	backends := ip.GetBackends(key)
	if len(backends) == 0 {
		return nil, ErrGetBackends
	}
	for _, be := range backends {
		if !be.IsActive() {
			return nil, fmt.Errorf("backend %s(%s) unavailable", be.Name, be.Url)
		}
	}
	bodies, _, err := QueryInParallel(backends, req, w, false)
	if err != nil {
		return nil, err
	}
	return bodies[0], nil
}

func QueryAlterQL(w http.ResponseWriter, req *http.Request, ip *Proxy) (body []byte, err error) {
	// all circles -> all backends -> create or drop database
	for _, circle := range ip.Circles {
		if !circle.IsActive() {
			return nil, fmt.Errorf("circle %d(%s) unavailable", circle.CircleId, circle.Name)
		}
	}
	backends := make([]*Backend, 0)
	for _, circle := range ip.Circles {
		backends = append(backends, circle.Backends...)
	}
	bodies, _, err := QueryInParallel(backends, req, w, false)
	if err != nil {
		return nil, err
	}
	return bodies[0], nil
}

func QueryInParallel(backends []*Backend, req *http.Request, w http.ResponseWriter, decompress bool) (bodies [][]byte, inactive int, err error) {
	var wg sync.WaitGroup
	var header http.Header
	req.Header.Set("Query-Origin", "Parallel")
	ch := make(chan *QueryResult, len(backends))
	for _, be := range backends {
		if !be.IsActive() {
			inactive++
			continue
		}
		wg.Add(1)
		go func(be *Backend) {
			defer wg.Done()
			cr := CloneQueryRequest(req)
			ch <- be.Query(cr, nil, decompress)
		}(be)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for qr := range ch {
		if qr.Err != nil {
			err = qr.Err
			return
		}
		header = qr.Header
		bodies = append(bodies, qr.Body)
	}
	if w != nil {
		CopyHeader(w.Header(), header)
	}
	return
}

func reduceByValues(bodies [][]byte) (rsp *Response, err error) {
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

func reduceBySeries(bodies [][]byte) (rsp *Response, err error) {
	var series models.Rows
	seriesMap := make(map[string]*models.Row)
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		for _, serie := range _series {
			seriesMap[serie.Name] = serie
		}
	}
	for _, serie := range seriesMap {
		series = append(series, serie)
	}
	return ResponseFromSeries(series), nil
}

func concatByValues(bodies [][]byte) (rsp *Response, err error) {
	var series models.Rows
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

func concatByResults(bodies [][]byte) (rsp *Response, err error) {
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
