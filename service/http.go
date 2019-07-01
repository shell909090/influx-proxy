// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"compress/gzip"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"
	"sync"

	"github.com/shell909090/influx-proxy/backend"
)

type HttpService struct {
	db            string
	ic            *backend.InfluxCluster
	cache_lock    sync.RWMutex
	write_through map[string]string
	params_cache  map[string]*url.Values
}

func NewHttpService(ic *backend.InfluxCluster, db string) (hs *HttpService) {
	hs = &HttpService{
		db:            db,
		ic:            ic,
		write_through: nil,
	}
	if hs.db != "" {
		log.Print("http database: ", hs.db)
	}
	return
}

func (hs *HttpService) Register(mux *http.ServeMux) {
	mux.HandleFunc("/reload", hs.HandlerReload)
	mux.HandleFunc("/ping", hs.HandlerPing)
	mux.HandleFunc("/query", hs.HandlerQuery)
	mux.HandleFunc("/write", hs.HandlerWrite)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
}

func (hs *HttpService) HandlerReload(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	err := hs.ic.LoadConfig()
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	version, err := hs.ic.Ping()
	if err != nil {
		panic("WTF")
		return
	}
	w.Header().Add("X-Influxdb-Version", version)
	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	db := req.FormValue("db")
	if hs.db != "" {
		if db != hs.db {
			w.WriteHeader(404)
			w.Write([]byte("database not exist."))
			return
		}
	}

	q := strings.TrimSpace(req.FormValue("q"))
	err := hs.ic.Query(w, req)
	if err != nil {
		log.Printf("query error: %s,the query is %s,the client is %s\n", err, q, req.RemoteAddr)
		return
	}
	if hs.ic.QueryTracing != 0 {
		log.Printf("the query is %s,the client is %s\n", q, req.RemoteAddr)
	}

	return
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	if req.Method != "POST" {
		w.WriteHeader(405)
		w.Write([]byte("method not allow."))
		return
	}

	// WARNING: don't modify query
	query := req.URL.Query()
	db := query.Get("db")

	if hs.db != "" {
		if db != hs.db {
			w.WriteHeader(404)
			w.Write([]byte("database not exist."))
			return
		}
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(req.Body)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte("unable to decode gzip body"))
			return
		}
		defer b.Close()
		body = b
	}

	p, err := ioutil.ReadAll(body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	rec := &backend.Record{
		Body: p,
	}
	if hs.write_through != nil {
		rec.Params = hs.PickParams(&query)
	}

	err = hs.ic.Write(rec)
	if err == nil {
		w.WriteHeader(204)
	}
	if hs.ic.WriteTracing != 0 {
		log.Printf("Write body received by handler: %s,the client is %s\n", p, req.RemoteAddr)
	}
	return
}

func (hs *HttpService) PickParams(query *url.Values) (p *url.Values) {
	var tmp *url.Values
	tmp = &url.Values{}
	for k, _ := range hs.write_through {
		if _, ok := (*query)[k]; ok {
			// This is why query shouldn't been modified.
			(*tmp)[k] = (*query)[k]
		}
	}

	if len(*tmp) == 0 {
		return nil
	}

	// TODO: is Encode stable?
	s := tmp.Encode()
	hs.cache_lock.RLock()
	p, ok := hs.params_cache[s]
	hs.cache_lock.RUnlock()
	if !ok {
		hs.cache_lock.Lock()
		hs.params_cache[s] = tmp
		hs.cache_lock.Unlock()
		p = tmp
	}
	return
}
