package service

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"

	"github.com/shell909090/influx-proxy/backend"
)

type HttpService struct {
	db string
	m  *backend.MultiAPI
}

func NewHttpService(m *backend.MultiAPI, db string) (hs *HttpService) {
	hs = &HttpService{
		db: db,
		m:  m,
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
}

func (hs *HttpService) HandlerReload(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	err := hs.m.LoadConfig()
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
	w.Header().Add("X-Influxdb-Version", backend.VERSION)
	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	w.Header().Add("X-Influxdb-Version", backend.VERSION)

	q := req.URL.Query()

	if hs.db != "" {
		db, ok := q["db"]
		if !ok || len(db) != 1 {
			w.WriteHeader(400)
			w.Write([]byte("illegal database."))
			return
		}

		if db[0] != hs.db {
			w.WriteHeader(404)
			w.Write([]byte("database not exist."))
			return
		}
	}

	err := hs.m.Query(w, req)
	if err != nil {
		log.Printf("query error: %s\n", err)
		return
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

	q := req.URL.Query()

	if hs.db != "" {
		db, ok := q["db"]
		if !ok || len(db) != 1 {
			w.WriteHeader(400)
			w.Write([]byte("illegal database."))
			return
		}

		if db[0] != hs.db {
			w.WriteHeader(404)
			w.Write([]byte("database not exist."))
			return
		}
	}

	p, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	err = hs.m.Write(p)
	if err == nil {
		w.WriteHeader(204)
	}
	return
}
