package service

import (
	"io/ioutil"
	"net/http"
	"net/http/pprof"

	"github.com/shell909090/influx-proxy/backend"
)

type HttpService struct {
	db  string
	api backend.InfluxAPI
}

func NewHttpService(api backend.InfluxAPI, db string) (hs *HttpService) {
	hs = &HttpService{
		db:  db,
		api: api,
	}
	return
}

func (hs *HttpService) Register(mux *http.ServeMux) {
	// TODO: reload
	mux.HandleFunc("/ping", hs.HandlerPing)
	// mux.HandleFunc("/query", hs.HandlerQuery)
	mux.HandleFunc("/write", hs.HandlerWrite)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", "1.1")
	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", "1.1")
	return
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", "1.1")

	if req.Method != "POST" {
		w.WriteHeader(405)
		w.Write([]byte("method not allow."))
		return
	}

	q := req.URL.Query()
	db, ok := q["db"]
	if !ok || len(db) != 1 {
		w.WriteHeader(404)
		w.Write([]byte("database empty."))
		return
	}

	if db[0] != hs.db {
		w.WriteHeader(404)
		w.Write([]byte("database not exist."))
		return
	}

	p, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	err = hs.api.Write(p)
	switch err {
	case nil:
		w.WriteHeader(204)
	}
	return
}
