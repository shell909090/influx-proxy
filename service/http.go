// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package service

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/http/pprof"
	"regexp"
	"strconv"
	"strings"

	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/service/prometheus"
	"github.com/chengshiwen/influx-proxy/service/prometheus/remote"
	"github.com/chengshiwen/influx-proxy/transfer"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
)

var (
	ErrInvalidTick    = errors.New("invalid tick, require non-negative integer")
	ErrInvalidWorker  = errors.New("invalid worker, require positive integer")
	ErrInvalidBatch   = errors.New("invalid batch, require positive integer")
	ErrInvalidLimit   = errors.New("invalid limit, require positive integer")
	ErrInvalidHaAddrs = errors.New("invalid ha_addrs, require at least two addresses as <host:port>, comma-separated")
)

type ServeMux struct {
	*http.ServeMux
}

func NewServeMux() *ServeMux {
	return &ServeMux{ServeMux: http.NewServeMux()}
}

func (mux *ServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Influxdb-Version", backend.Version)
	w.Header().Add("X-Influxdb-Build", "InfluxDB Proxy")
	mux.ServeMux.ServeHTTP(w, r)
}

type HttpService struct { // nolint:golint
	ip           *backend.Proxy
	tx           *transfer.Transfer
	username     string
	password     string
	authEncrypt  bool
	writeTracing bool
	queryTracing bool
	pprofEnabled bool
}

func NewHttpService(cfg *backend.ProxyConfig) (hs *HttpService) { // nolint:golint
	ip := backend.NewProxy(cfg)
	hs = &HttpService{
		ip:           ip,
		tx:           transfer.NewTransfer(cfg, ip.Circles),
		username:     cfg.Username,
		password:     cfg.Password,
		authEncrypt:  cfg.AuthEncrypt,
		writeTracing: cfg.WriteTracing,
		queryTracing: cfg.QueryTracing,
		pprofEnabled: cfg.PprofEnabled,
	}
	return
}

func (hs *HttpService) Register(mux *ServeMux) {
	mux.HandleFunc("/ping", hs.HandlerPing)
	mux.HandleFunc("/query", hs.HandlerQuery)
	mux.HandleFunc("/write", hs.HandlerWrite)
	mux.HandleFunc("/api/v2/query", hs.HandlerQueryV2)
	mux.HandleFunc("/api/v2/write", hs.HandlerWriteV2)
	mux.HandleFunc("/health", hs.HandlerHealth)
	mux.HandleFunc("/replica", hs.HandlerReplica)
	mux.HandleFunc("/encrypt", hs.HandlerEncrypt)
	mux.HandleFunc("/decrypt", hs.HandlerDecrypt)
	mux.HandleFunc("/rebalance", hs.HandlerRebalance)
	mux.HandleFunc("/recovery", hs.HandlerRecovery)
	mux.HandleFunc("/resync", hs.HandlerResync)
	mux.HandleFunc("/cleanup", hs.HandlerCleanup)
	mux.HandleFunc("/transfer/state", hs.HandlerTransferState)
	mux.HandleFunc("/transfer/stats", hs.HandlerTransferStats)
	mux.HandleFunc("/api/v1/prom/read", hs.HandlerPromRead)
	mux.HandleFunc("/api/v1/prom/write", hs.HandlerPromWrite)
	if hs.pprofEnabled {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	}
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	db := req.FormValue("db")
	q := req.FormValue("q")
	body, err := hs.ip.Query(w, req)
	if err != nil {
		log.Printf("influxql query error: %s, query: %s, db: %s, client: %s", err, q, db, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	hs.WriteBody(w, body)
	if hs.queryTracing {
		log.Printf("influxql query: %s, db: %s, client: %s", q, db, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerQueryV2(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	var contentType = "application/json"
	if ct := req.Header.Get("Content-Type"); ct != "" {
		contentType = ct
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rbody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	qr := &backend.QueryRequest{}
	switch mt {
	case "application/vnd.flux":
		qr.Query = string(rbody)
	case "application/json":
		fallthrough
	default:
		if err = json.Unmarshal(rbody, qr); err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("failed parsing request body as JSON: %s", err))
			return
		}
	}

	if qr.Query == "" && qr.Spec == nil {
		hs.WriteError(w, req, http.StatusBadRequest, "request body requires either spec or query")
		return
	}
	if qr.Type != "" && qr.Type != "flux" {
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("unknown query type: %s", qr.Type))
		return
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(rbody))
	err = hs.ip.QueryFlux(w, req, qr)
	if err != nil {
		log.Printf("flux query error: %s, query: %s, spec: %s, client: %s", err, qr.Query, qr.Spec, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if hs.queryTracing {
		log.Printf("flux query: %s, spec: %s, client: %s", qr.Query, qr.Spec, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
		if precision == "" {
			precision = "ns"
		}
	default:
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use n, ns, u, ms, s, m or h)", precision))
		return
	}

	db, err := hs.queryDB(req, false)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rp := req.URL.Query().Get("rp")

	hs.handlerWrite(db, rp, precision, w, req)
}

func (hs *HttpService) HandlerWriteV2(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	switch precision {
	case "ns":
		precision = "n"
	case "us":
		precision = "u"
	case "ms", "s", "":
		// same as v1 so do nothing
	default:
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision))
		return
	}

	db, rp, err := hs.bucket2dbrp(req.URL.Query().Get("bucket"))
	if err != nil {
		hs.WriteError(w, req, http.StatusNotFound, err.Error())
		return
	}
	if hs.ip.IsForbiddenDB(db) {
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("database forbidden: %s", db))
		return
	}

	hs.handlerWrite(db, rp, precision, w, req)
}

func (hs *HttpService) handlerWrite(db, rp, precision string, w http.ResponseWriter, req *http.Request) {
	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	err = hs.ip.Write(p, db, rp, precision)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
	}
	if hs.writeTracing {
		log.Printf("write line protocol, db: %s, rp: %s, precision: %s, data: %s, client: %s", db, rp, precision, p, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}
	stats := req.URL.Query().Get("stats") == "true"
	resp := map[string]interface{}{
		"name":    "influx-proxy",
		"message": "ready for queries and writes",
		"status":  "pass",
		"checks":  []string{},
		"circles": hs.ip.GetHealth(stats),
		"version": backend.Version,
	}
	hs.Write(w, req, http.StatusOK, resp)
}

func (hs *HttpService) HandlerReplica(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	db := req.URL.Query().Get("db")
	meas := req.URL.Query().Get("meas")
	if db != "" && meas != "" {
		key := backend.GetKey(db, meas)
		backends := hs.ip.GetBackends(key)
		data := make([]map[string]interface{}, len(backends))
		for i, b := range backends {
			c := hs.ip.Circles[i]
			data[i] = map[string]interface{}{
				"backend": map[string]string{"name": b.Name, "url": b.Url},
				"circle":  map[string]interface{}{"id": c.CircleId, "name": c.Name},
			}
		}
		hs.Write(w, req, http.StatusOK, data)
	} else {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid db or meas")
	}
}

func (hs *HttpService) HandlerEncrypt(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	text := req.URL.Query().Get("text")
	encrypt := util.AesEncrypt(text)
	hs.WriteText(w, http.StatusOK, encrypt)
}

func (hs *HttpService) HandlerDecrypt(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	key := req.URL.Query().Get("key")
	text := req.URL.Query().Get("text")
	if !util.CheckCipherKey(key) {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid key")
		return
	}
	decrypt := util.AesDecrypt(text)
	hs.WriteText(w, http.StatusOK, decrypt)
}

func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	operation := req.FormValue("operation")
	if operation != "add" && operation != "rm" {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid operation")
		return
	}

	var backends []*backend.Backend
	if operation == "rm" {
		var body struct {
			Backends []*backend.BackendConfig `json:"backends"`
		}
		decoder := json.NewDecoder(req.Body)
		err := decoder.Decode(&body)
		if err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, "invalid backends from body")
			return
		}
		for _, bkcfg := range body.Backends {
			backends = append(backends, backend.NewSimpleBackend(bkcfg))
			hs.tx.CircleStates[circleId].Stats[bkcfg.Url] = &transfer.Stats{}
		}
	}
	backends = append(backends, hs.ip.Circles[circleId].Backends...)

	if hs.tx.CircleStates[circleId].Transferring {
		hs.WriteText(w, http.StatusBadRequest, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, http.StatusBadRequest, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	dbs := hs.formValues(req, "dbs")
	go hs.tx.Rebalance(circleId, backends, dbs)
	hs.WriteText(w, http.StatusAccepted, "accepted")
}

func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	fromCircleId, err := hs.formCircleId(req, "from_circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	toCircleId, err := hs.formCircleId(req, "to_circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if fromCircleId == toCircleId {
		hs.WriteError(w, req, http.StatusBadRequest, "from_circle_id and to_circle_id cannot be same")
		return
	}

	if hs.tx.CircleStates[fromCircleId].Transferring || hs.tx.CircleStates[toCircleId].Transferring {
		hs.WriteText(w, http.StatusBadRequest, fmt.Sprintf("circle %d or %d is transferring", fromCircleId, toCircleId))
		return
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, http.StatusBadRequest, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	backendUrls := hs.formValues(req, "backend_urls")
	dbs := hs.formValues(req, "dbs")
	go hs.tx.Recovery(fromCircleId, toCircleId, backendUrls, dbs)
	hs.WriteText(w, http.StatusAccepted, "accepted")
}

func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	tick, err := hs.formTick(req)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	for _, cs := range hs.tx.CircleStates {
		if cs.Transferring {
			hs.WriteText(w, http.StatusBadRequest, fmt.Sprintf("circle %d is transferring", cs.CircleId))
			return
		}
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, http.StatusBadRequest, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	dbs := hs.formValues(req, "dbs")
	go hs.tx.Resync(dbs, tick)
	hs.WriteText(w, http.StatusAccepted, "accepted")
}

func (hs *HttpService) HandlerCleanup(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	if hs.tx.CircleStates[circleId].Transferring {
		hs.WriteText(w, http.StatusBadRequest, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, http.StatusBadRequest, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	go hs.tx.Cleanup(circleId)
	hs.WriteText(w, http.StatusAccepted, "accepted")
}

func (hs *HttpService) HandlerTransferState(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	if req.Method == "GET" {
		data := make([]map[string]interface{}, len(hs.tx.CircleStates))
		for k, cs := range hs.tx.CircleStates {
			data[k] = map[string]interface{}{
				"id":           cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		state := map[string]interface{}{"resyncing": hs.tx.Resyncing, "circles": data}
		hs.Write(w, req, http.StatusOK, state)
		return
	} else if req.Method == "POST" {
		state := make(map[string]interface{})
		if req.FormValue("resyncing") != "" {
			resyncing, err := hs.formBool(req, "resyncing")
			if err != nil {
				hs.WriteError(w, req, http.StatusBadRequest, "illegal resyncing")
				return
			}
			hs.tx.Resyncing = resyncing
			state["resyncing"] = resyncing
		}
		if req.FormValue("circle_id") != "" || req.FormValue("transferring") != "" {
			circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
			if err != nil {
				hs.WriteError(w, req, http.StatusBadRequest, err.Error())
				return
			}
			transferring, err := hs.formBool(req, "transferring")
			if err != nil {
				hs.WriteError(w, req, http.StatusBadRequest, "illegal transferring")
				return
			}
			cs := hs.tx.CircleStates[circleId]
			cs.Transferring = transferring
			cs.SetTransferIn(transferring)
			state["circle"] = map[string]interface{}{
				"id":           cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		if len(state) == 0 {
			hs.WriteError(w, req, http.StatusBadRequest, "missing query parameter")
			return
		}
		hs.Write(w, req, http.StatusOK, state)
		return
	}
}

func (hs *HttpService) HandlerTransferStats(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	statsType := req.FormValue("type")
	if statsType == "rebalance" || statsType == "recovery" || statsType == "resync" || statsType == "cleanup" {
		hs.Write(w, req, http.StatusOK, hs.tx.CircleStates[circleId].Stats)
	} else {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid stats type")
	}
}

func (hs *HttpService) HandlerPromRead(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	db, err := hs.queryDB(req, true)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		hs.WriteError(w, req, http.StatusInternalServerError, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	var readReq remote.ReadRequest
	if err = proto.Unmarshal(reqBuf, &readReq); err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if len(readReq.Queries) != 1 {
		err = errors.New("prometheus read endpoint currently only supports one query at a time")
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	var metric string
	q := readReq.Queries[0]
	for _, m := range q.Matchers {
		if m.Name == "__name__" {
			metric = m.Value
		}
	}
	if metric == "" {
		log.Printf("prometheus query: %v", q)
		err = errors.New("prometheus metric not found")
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(compressed))
	err = hs.ip.ReadProm(w, req, db, metric)
	if err != nil {
		log.Printf("prometheus read error: %s, query: %s %s %v, client: %s", err, req.Method, db, q, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if hs.queryTracing {
		log.Printf("prometheus read: %s %s %v, client: %s", req.Method, db, q, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerPromWrite(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	db, err := hs.queryDB(req, false)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rp := req.URL.Query().Get("rp")

	body := req.Body
	var bs []byte
	if req.ContentLength > 0 {
		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, req.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err = buf.ReadFrom(body)
	if err != nil {
		if hs.writeTracing {
			log.Printf("prom write handler unable to read bytes from request body")
		}
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		if hs.writeTracing {
			log.Printf("prom write handler unable to snappy decode from request body, error: %s", err)
		}
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var writeReq remote.WriteRequest
	if err = proto.Unmarshal(reqBuf, &writeReq); err != nil {
		if hs.writeTracing {
			log.Printf("prom write handler unable to unmarshal from snappy decoded bytes, error: %s", err)
		}
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	points, err := prometheus.WriteRequestToPoints(&writeReq)
	if err != nil {
		if hs.writeTracing {
			log.Printf("prom write handler, error: %s", err)
		}
		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			hs.WriteError(w, req, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Write points.
	err = hs.ip.WritePoints(points, db, rp)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (hs *HttpService) Write(w http.ResponseWriter, req *http.Request, status int, data interface{}) {
	if status/100 >= 4 {
		hs.WriteError(w, req, status, data.(string))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(data, pretty))
}

func (hs *HttpService) WriteError(w http.ResponseWriter, req *http.Request, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Influxdb-Error", err)
	w.WriteHeader(status)
	rsp := backend.ResponseFromError(err)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(rsp, pretty))
}

func (hs *HttpService) WriteBody(w http.ResponseWriter, body []byte) {
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (hs *HttpService) WriteText(w http.ResponseWriter, status int, text string) {
	w.WriteHeader(status)
	w.Write([]byte(text + "\n"))
}

func (hs *HttpService) checkMethodAndAuth(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	return hs.checkMethod(w, req, methods...) && hs.checkAuth(w, req)
}

func (hs *HttpService) checkMethod(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	for _, method := range methods {
		if req.Method == method {
			return true
		}
	}
	hs.WriteError(w, req, http.StatusMethodNotAllowed, "method not allow")
	return false
}

func (hs *HttpService) checkAuth(w http.ResponseWriter, req *http.Request) bool {
	if hs.username == "" && hs.password == "" {
		return true
	}
	q := req.URL.Query()
	if u, p := q.Get("u"), q.Get("p"); hs.compareAuth(u, p) {
		return true
	}
	if u, p, ok := req.BasicAuth(); ok && hs.compareAuth(u, p) {
		return true
	}
	if u, p, ok := hs.parseAuth(req); ok && hs.compareAuth(u, p) {
		return true
	}
	hs.WriteError(w, req, http.StatusUnauthorized, "authentication failed")
	return false
}

func (hs *HttpService) parseAuth(req *http.Request) (string, string, bool) {
	if auth := req.Header.Get("Authorization"); auth != "" {
		items := strings.Split(auth, " ")
		if len(items) == 2 && items[0] == "Token" {
			token := items[1]
			i := strings.IndexByte(token, ':')
			if i >= 0 {
				return token[:i], token[i+1:], true
			}
		}
	}
	return "", "", false
}

func (hs *HttpService) compareAuth(u, p string) bool {
	return hs.transAuth(u) == hs.username && hs.transAuth(p) == hs.password
}

func (hs *HttpService) transAuth(text string) string {
	if hs.authEncrypt {
		return util.AesEncrypt(text)
	}
	return text
}

func (hs *HttpService) bucket2dbrp(bucket string) (string, string, error) {
	// test for a slash in our bucket name.
	switch idx := strings.IndexByte(bucket, '/'); idx {
	case -1:
		// if there is no slash, we're mapping bucket to the database.
		switch db := bucket; db {
		case "":
			// if our "database" is an empty string, this is an error.
			return "", "", fmt.Errorf(`bucket name %q is missing a slash; not in "database/retention-policy" format`, bucket)
		default:
			return db, "", nil
		}
	default:
		// there is a slash
		switch db, rp := bucket[:idx], bucket[idx+1:]; {
		case db == "":
			// empty database is unrecoverable
			return "", "", fmt.Errorf(`bucket name %q is in db/rp form but has an empty database`, bucket)
		default:
			return db, rp, nil
		}
	}
}

func (hs *HttpService) queryDB(req *http.Request, form bool) (string, error) {
	var db string
	if form {
		db = req.FormValue("db")
	} else {
		db = req.URL.Query().Get("db")
	}
	if db == "" {
		return db, errors.New("database not found")
	}
	if hs.ip.IsForbiddenDB(db) {
		return db, fmt.Errorf("database forbidden: %s", db)
	}
	return db, nil
}

func (hs *HttpService) formValues(req *http.Request, key string) []string {
	var values []string
	str := strings.Trim(req.FormValue(key), ", ")
	if str != "" {
		values = strings.Split(str, ",")
	}
	return values
}

func (hs *HttpService) formBool(req *http.Request, key string) (bool, error) {
	return strconv.ParseBool(req.FormValue(key))
}

func (hs *HttpService) formTick(req *http.Request) (int64, error) {
	str := strings.TrimSpace(req.FormValue("tick"))
	if str == "" {
		return 0, nil
	}
	tick, err := strconv.ParseInt(str, 10, 64)
	if err != nil || tick < 0 {
		return 0, ErrInvalidTick
	}
	return tick, nil
}

func (hs *HttpService) formCircleId(req *http.Request, key string) (int, error) { // nolint:golint
	circleId, err := strconv.Atoi(req.FormValue(key)) // nolint:golint
	if err != nil || circleId < 0 || circleId >= len(hs.ip.Circles) {
		return circleId, fmt.Errorf("invalid %s", key)
	}
	return circleId, nil
}

func (hs *HttpService) setParam(req *http.Request) error {
	var err error
	err = hs.setWorker(req)
	if err != nil {
		return err
	}
	err = hs.setBatch(req)
	if err != nil {
		return err
	}
	err = hs.setLimit(req)
	if err != nil {
		return err
	}
	err = hs.setHaAddrs(req)
	if err != nil {
		return err
	}
	return nil
}

func (hs *HttpService) setWorker(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("worker"))
	if str != "" {
		worker, err := strconv.Atoi(str)
		if err != nil || worker <= 0 {
			return ErrInvalidWorker
		}
		hs.tx.Worker = worker
	} else {
		hs.tx.Worker = transfer.DefaultWorker
	}
	return nil
}

func (hs *HttpService) setBatch(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("batch"))
	if str != "" {
		batch, err := strconv.Atoi(str)
		if err != nil || batch <= 0 {
			return ErrInvalidBatch
		}
		hs.tx.Batch = batch
	} else {
		hs.tx.Batch = transfer.DefaultBatch
	}
	return nil
}

func (hs *HttpService) setLimit(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("limit"))
	if str != "" {
		limit, err := strconv.Atoi(str)
		if err != nil || limit <= 0 {
			return ErrInvalidLimit
		}
		hs.tx.Limit = limit
	} else {
		hs.tx.Limit = transfer.DefaultLimit
	}
	return nil
}

func (hs *HttpService) setHaAddrs(req *http.Request) error {
	haAddrs := hs.formValues(req, "ha_addrs")
	if len(haAddrs) > 1 {
		r, _ := regexp.Compile(`^[\w-.]+:\d{1,5}$`)
		for _, addr := range haAddrs {
			if !r.MatchString(addr) {
				return ErrInvalidHaAddrs
			}
		}
		hs.tx.HaAddrs = haAddrs
	} else if len(haAddrs) == 1 {
		return ErrInvalidHaAddrs
	}
	return nil
}
