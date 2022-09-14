// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"errors"
	"fmt"
	"log"
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

func query(w http.ResponseWriter, req *http.Request, ip *Proxy, key string, fn func(*Backend, *http.Request, http.ResponseWriter) ([]byte, error)) (body []byte, err error) {
	// pass non-active, rewriting or write-only.
	perms := rand.Perm(len(ip.Circles))
	for _, p := range perms {
		be := ip.Circles[p].GetBackend(key)
		if !be.IsActive() || be.IsRewriting() || be.IsWriteOnly() {
			continue
		}
		body, err = fn(be, req, w)
		if err == nil {
			return
		}
	}

	// pass non-active, non-writing (excluding rewriting and write-only).
	backends := ip.GetBackends(key)
	for _, be := range backends {
		if !be.IsActive() || !(be.IsRewriting() || be.IsWriteOnly()) {
			continue
		}
		body, err = fn(be, req, w)
		if err == nil {
			return
		}
	}

	if err != nil {
		return
	}
	return nil, ErrBackendsUnavailable
}

func ReadProm(w http.ResponseWriter, req *http.Request, ip *Proxy, db, meas string) (err error) {
	// all circles -> backend by key(db,meas) -> select or show
	key := GetKey(db, meas)
	fn := func(be *Backend, req *http.Request, w http.ResponseWriter) ([]byte, error) {
		err = be.ReadProm(req, w)
		return nil, err
	}
	_, err = query(w, req, ip, key, fn)
	return
}

func QueryFlux(w http.ResponseWriter, req *http.Request, ip *Proxy, bucket, meas string) (err error) {
	// all circles -> backend by key(org,bucket,meas) -> query flux
	key := GetKey(bucket, meas)
	fn := func(be *Backend, req *http.Request, w http.ResponseWriter) ([]byte, error) {
		err = be.QueryFlux(req, w)
		return nil, err
	}
	_, err = query(w, req, ip, key, fn)
	return
}

func QueryFromQL(w http.ResponseWriter, req *http.Request, ip *Proxy, tokens []string, db string) (body []byte, err error) {
	// all circles -> backend by key(db,meas) -> select or show
	meas, err := GetMeasurementFromTokens(tokens)
	if err != nil {
		return nil, ErrGetMeasurement
	}
	key := GetKey(db, meas)
	fn := func(be *Backend, req *http.Request, w http.ResponseWriter) ([]byte, error) {
		qr := be.Query(req, w, false)
		return qr.Body, qr.Err
	}
	body, err = query(w, req, ip, key, fn)
	return
}

func QueryShowQL(w http.ResponseWriter, req *http.Request, ip *Proxy, tokens []string) (body []byte, err error) {
	// all circles -> all backends -> show
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	backends := ip.GetAllBackends()
	bodies, inactive, err := QueryInParallel(backends, req, w, true)
	if err != nil {
		return
	}
	if inactive > 0 {
		log.Printf("query: %s, inactive: %d/%d backends unavailable", req.FormValue("q"), inactive, inactive+len(bodies))
		if len(bodies) == 0 {
			return nil, ErrBackendsUnavailable
		}
	}

	var rsp *Response
	stmt2 := GetHeadStmtFromTokens(tokens, 2)
	stmt3 := GetHeadStmtFromTokens(tokens, 3)
	if stmt2 == "show measurements" || stmt2 == "show series" || stmt2 == "show databases" {
		rsp, err = reduceByValues(bodies)
	} else if stmt3 == "show field keys" || stmt3 == "show tag keys" || stmt3 == "show tag values" {
		rsp, err = reduceBySeries(bodies)
	} else if stmt3 == "show retention policies" {
		rsp, err = attachByValues(bodies)
	} else if stmt2 == "show stats" {
		rsp, err = concatByResults(bodies)
	}
	if err != nil {
		return
	}
	if rsp == nil {
		rsp = ResponseFromSeries(nil)
	}
	pretty := req.URL.Query().Get("pretty") == "true"
	body = util.MarshalJSON(rsp, pretty)
	if w.Header().Get("Content-Encoding") == "gzip" {
		var buf bytes.Buffer
		err = Compress(&buf, body)
		if err != nil {
			return
		}
		body = buf.Bytes()
	}
	w.Header().Del("Content-Length")
	return
}

func QueryDeleteOrDropQL(w http.ResponseWriter, req *http.Request, ip *Proxy, tokens []string, db string) (body []byte, err error) {
	// all circles -> backend by key(db,meas) -> delete or drop measurement/series
	meas, err := GetMeasurementFromTokens(tokens)
	if err != nil {
		return nil, err
	}
	key := GetKey(db, meas)
	backends := ip.GetBackends(key)
	return QueryBackends(backends, req, w)
}

func QueryAlterQL(w http.ResponseWriter, req *http.Request, ip *Proxy) (body []byte, err error) {
	// all circles -> all backends -> create or drop database; create, alter or drop retention policy
	backends := ip.GetAllBackends()
	return QueryBackends(backends, req, w)
}

func QueryBackends(backends []*Backend, req *http.Request, w http.ResponseWriter) (body []byte, err error) {
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

func QueryInParallel(backends []*Backend, req *http.Request, w http.ResponseWriter, decompress bool) (bodies [][]byte, inactive int, err error) {
	var wg sync.WaitGroup
	var header http.Header
	req.Header.Set(HeaderQueryOrigin, QueryParallel)
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

func attachByValues(bodies [][]byte) (rsp *Response, err error) {
	var series models.Rows
	valuesMap := make(map[string]bool)
	isInitial := false
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		if len(_series) == 1 {
			if series == nil {
				series = _series
			}
			for _, value := range _series[0].Values {
				key := value[0].(string)
				if !isInitial {
					valuesMap[key] = true
					continue
				}
				if _, ok := valuesMap[key]; !ok {
					series[0].Values = append(series[0].Values, value)
					valuesMap[key] = true
				}
			}
			isInitial = true
		}
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
