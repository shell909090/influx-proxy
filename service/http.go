package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/transfer"
	"github.com/chengshiwen/influx-proxy/util"
	gzip "github.com/klauspost/pgzip"
)

type HttpService struct { // nolint:golint
	ip           *backend.Proxy
	tx           *transfer.Transfer
	DBSet        map[string]bool
	Username     string
	Password     string
	AuthSecure   bool
	LogEnabled   bool
	WriteTracing bool
	QueryTracing bool
}

func NewHttpService(cfg *backend.ProxyConfig) (hs *HttpService) { // nolint:golint
	ip := backend.NewProxy(cfg)
	hs = &HttpService{
		ip:           ip,
		tx:           transfer.NewTransfer(cfg, ip.Circles),
		DBSet:        make(map[string]bool),
		Username:     cfg.Username,
		Password:     cfg.Password,
		AuthSecure:   cfg.AuthSecure,
		LogEnabled:   cfg.LogEnabled,
		WriteTracing: cfg.WriteTracing,
		QueryTracing: cfg.QueryTracing,
	}
	for _, db := range cfg.DBList {
		hs.DBSet[db] = true
	}
	return
}

func (hs *HttpService) Register(mux *http.ServeMux) {
	mux.HandleFunc("/ping", hs.HandlerPing)
	mux.HandleFunc("/query", hs.HandlerQuery)
	mux.HandleFunc("/write", hs.HandlerWrite)
	mux.HandleFunc("/health", hs.HandlerHealth)
	mux.HandleFunc("/replica", hs.HandlerReplica)
	mux.HandleFunc("/encrypt", hs.HandlerEncrypt)
	mux.HandleFunc("/decrypt", hs.HandlerDencrypt)
	mux.HandleFunc("/rebalance", hs.HandlerRebalance)
	mux.HandleFunc("/recovery", hs.HandlerRecovery)
	mux.HandleFunc("/resync", hs.HandlerResync)
	mux.HandleFunc("/cleanup", hs.HandlerCleanup)
	mux.HandleFunc("/transfer/state", hs.HandlerTransferState)
	mux.HandleFunc("/transfer/stats", hs.HandlerTransferStats)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addHeader(w)
	w.WriteHeader(204)
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET", "POST"}) {
		return
	}

	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		hs.Write(w, 400, "query not found")
		return
	}
	if hs.QueryTracing {
		log.Printf("query: %s %s %s, client: %s", req.Method, req.FormValue("db"), q, req.RemoteAddr)
	}

	tokens, check := backend.CheckQuery(q)
	if !check {
		hs.Write(w, 400, "query forbidden")
		return
	}

	checkDb, showDb, alterDb, db := backend.CheckDatabaseFromTokens(tokens)
	if !checkDb {
		db = req.FormValue("db")
		if db == "" {
			db, _ = backend.GetDatabaseFromTokens(tokens)
		}
	}
	if !showDb {
		if db == "" {
			hs.Write(w, 400, "database not found")
			return
		}
		if len(hs.DBSet) > 0 && !hs.DBSet[db] {
			hs.Write(w, 400, fmt.Sprintf("database forbidden: %s", db))
			return
		}
	}

	body, err := hs.ip.Query(w, req, tokens, db, alterDb)
	if err != nil {
		log.Printf("query error: %s, the query is %s, db is %s", err, q, db)
		hs.Write(w, 400, fmt.Sprintf("query error: %s", err))
		return
	}
	w.Write(body)
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	precision := req.URL.Query().Get("precision")
	if precision == "" {
		precision = "ns"
	}
	db := req.URL.Query().Get("db")
	if db == "" {
		hs.Write(w, 400, "database not found")
		return
	}
	if len(hs.DBSet) > 0 && !hs.DBSet[db] {
		hs.Write(w, 400, fmt.Sprintf("database forbidden: %s", db))
		return
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			hs.Write(w, 400, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	err = hs.ip.Write(p, db, precision)
	if err == nil {
		w.WriteHeader(204)
	}
	if hs.WriteTracing {
		log.Printf("write: %s %s %s, client: %s", db, precision, p, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET"}) {
		return
	}

	hs.addJSONHeader(w)
	data := make([]map[string]interface{}, len(hs.ip.Circles))
	for i, c := range hs.ip.Circles {
		data[i] = map[string]interface{}{
			"circle":   map[string]interface{}{"id": c.CircleId, "name": c.Name, "write_only": c.WriteOnly},
			"backends": c.GetHealth(),
		}
	}
	pretty := req.URL.Query().Get("pretty") == "true"
	res := util.MarshalJSON(data, pretty, true)
	w.Write(res)
}

func (hs *HttpService) HandlerReplica(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET"}) {
		return
	}

	db := req.FormValue("db")
	meas := req.FormValue("meas")
	if db != "" && meas != "" {
		hs.addJSONHeader(w)
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
		pretty := req.URL.Query().Get("pretty") == "true"
		res := util.MarshalJSON(data, pretty, true)
		w.Write(res)
	} else {
		hs.Write(w, 400, "invalid db or meas")
	}
}

func (hs *HttpService) HandlerEncrypt(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethod(w, req, []string{"GET"}) {
		return
	}
	msg := req.URL.Query().Get("msg")
	encrypt := util.AesEncrypt(msg)
	w.Write([]byte(encrypt + "\n"))
}

func (hs *HttpService) HandlerDencrypt(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethod(w, req, []string{"GET"}) {
		return
	}
	key := req.URL.Query().Get("key")
	msg := req.URL.Query().Get("msg")
	if !util.CheckCipherKey(key) {
		hs.Write(w, 400, "invalid key")
		return
	}
	decrypt := util.AesDecrypt(msg)
	w.Write([]byte(decrypt + "\n"))
}

func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}
	operation := req.FormValue("operation")
	if operation != "add" && operation != "rm" {
		hs.Write(w, 400, "invalid operation")
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
			hs.Write(w, 400, "invalid backends from body")
			return
		}
		for _, bkcfg := range body.Backends {
			backends = append(backends, backend.NewSimpleBackend(bkcfg))
			hs.tx.CircleStates[circleId].Stats[bkcfg.Url] = &transfer.Stats{}
		}
	}
	backends = append(backends, hs.ip.Circles[circleId].Backends...)

	if hs.tx.CircleStates[circleId].Transferring {
		hs.Write(w, 202, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.Write(w, 202, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	dbs := hs.formValues(req, "db")
	go hs.tx.Rebalance(circleId, backends, dbs)
	hs.Write(w, 202, "accepted")
}

func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	fromCircleId, err := hs.formCircleId(req, "from_circle_id") // nolint:golint
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}
	toCircleId, err := hs.formCircleId(req, "to_circle_id") // nolint:golint
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}
	if fromCircleId == toCircleId {
		hs.Write(w, 400, "from_circle_id and to_circle_id cannot be same")
		return
	}

	if hs.tx.CircleStates[fromCircleId].Transferring || hs.tx.CircleStates[toCircleId].Transferring {
		hs.Write(w, 202, fmt.Sprintf("circle %d or %d is transferring", fromCircleId, toCircleId))
		return
	}
	if hs.tx.Resyncing {
		hs.Write(w, 202, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	backendUrls := hs.formValues(req, "backend_urls")
	dbs := hs.formValues(req, "db")
	go hs.tx.Recovery(fromCircleId, toCircleId, backendUrls, dbs)
	hs.Write(w, 202, "accepted")
}

func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	seconds, err := hs.formSeconds(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	for _, cs := range hs.tx.CircleStates {
		if cs.Transferring {
			hs.Write(w, 202, fmt.Sprintf("circle %d is transferring", cs.CircleId))
			return
		}
	}
	if hs.tx.Resyncing {
		hs.Write(w, 202, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	dbs := hs.formValues(req, "db")
	go hs.tx.Resync(dbs, seconds)
	hs.Write(w, 202, "accepted")
}

func (hs *HttpService) HandlerCleanup(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	if hs.tx.CircleStates[circleId].Transferring {
		hs.Write(w, 202, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.Write(w, 202, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	go hs.tx.Cleanup(circleId)
	hs.Write(w, 202, "accepted")
}

func (hs *HttpService) HandlerTransferState(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET", "POST"}) {
		return
	}

	pretty := req.URL.Query().Get("pretty") == "true"
	if req.Method == "GET" {
		hs.addJSONHeader(w)
		data := make([]map[string]interface{}, len(hs.tx.CircleStates))
		for k, cs := range hs.tx.CircleStates {
			data[k] = map[string]interface{}{
				"id":           cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		state := map[string]interface{}{"resyncing": hs.tx.Resyncing, "circles": data}
		res := util.MarshalJSON(state, pretty, true)
		w.Write(res)
		return
	} else if req.Method == "POST" {
		state := make(map[string]interface{})
		if req.FormValue("resyncing") != "" {
			resyncing, err := hs.formBool(req, "resyncing")
			if err != nil {
				hs.Write(w, 400, "illegal resyncing")
				return
			}
			hs.tx.Resyncing = resyncing
			state["resyncing"] = resyncing
		}
		if req.FormValue("circle_id") != "" || req.FormValue("transferring") != "" {
			circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
			if err != nil {
				hs.Write(w, 400, err.Error())
				return
			}
			transferring, err := hs.formBool(req, "transferring")
			if err != nil {
				hs.Write(w, 400, "illegal transferring")
				return
			}
			cs := hs.tx.CircleStates[circleId]
			cs.Transferring = transferring
			cs.WriteOnly = transferring
			state["circle"] = map[string]interface{}{
				"id":           cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		if len(state) == 0 {
			hs.Write(w, 400, "missing query parameter")
			return
		}
		hs.addJSONHeader(w)
		res := util.MarshalJSON(state, pretty, true)
		w.Write(res)
		return
	}
}

func (hs *HttpService) HandlerTransferStats(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET"}) {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.Write(w, 400, err.Error())
		return
	}

	statsType := req.FormValue("type")
	if statsType == "rebalance" || statsType == "recovery" || statsType == "resync" || statsType == "cleanup" {
		hs.addJSONHeader(w)
		pretty := req.URL.Query().Get("pretty") == "true"
		res := util.MarshalJSON(hs.tx.CircleStates[circleId].Stats, pretty, true)
		w.Write(res)
	} else {
		hs.Write(w, 400, "invalid stats type")
	}
}

func (hs *HttpService) addHeader(w http.ResponseWriter) {
	hs.addVerHeader(w)
	hs.addJSONHeader(w)
}

func (hs *HttpService) addVerHeader(w http.ResponseWriter) {
	w.Header().Add("X-Influxdb-Version", backend.VERSION)
}

func (hs *HttpService) addJSONHeader(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "application/json")
}

func (hs *HttpService) Write(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	if w.Header().Get("Content-Type") == "application/json" {
		rsp := backend.ResponseFromError(msg, true)
		w.Write(util.MarshalJSON(rsp, false, true))
	} else {
		w.Write([]byte(msg + "\n"))
	}
	if hs.LogEnabled {
		log.Print(msg)
	}
}

func (hs *HttpService) checkMethodAndAuth(w http.ResponseWriter, req *http.Request, methods []string) bool {
	return hs.checkMethod(w, req, methods) && hs.checkAuth(w, req)
}

func (hs *HttpService) checkMethod(w http.ResponseWriter, req *http.Request, methods []string) bool {
	for _, method := range methods {
		if req.Method == method {
			return true
		}
	}
	hs.Write(w, 405, "method not allow")
	return false
}

func (hs *HttpService) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if hs.Username == "" && hs.Password == "" {
		return true
	}
	u, p := r.URL.Query().Get("u"), r.URL.Query().Get("p")
	if hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
		return true
	}
	u, p, ok := r.BasicAuth()
	if ok && hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
		return true
	}
	hs.Write(w, 401, "authentication failed")
	return false
}

func (hs *HttpService) transAuth(msg string) string {
	if hs.AuthSecure {
		return util.AesEncrypt(msg)
	}
	return msg
}

func (hs *HttpService) formValues(req *http.Request, key string) []string {
	var values []string
	str := strings.Trim(req.FormValue(key), ", ")
	if str != "" {
		values = strings.Split(str, ",")
	}
	return values
}

func (hs *HttpService) formPositiveInt(req *http.Request, key string) (int, bool) {
	str := strings.TrimSpace(req.FormValue(key))
	if str == "" {
		return 0, true
	}
	value, err := strconv.Atoi(str)
	return value, err == nil && value >= 0
}

func (hs *HttpService) formSeconds(req *http.Request) (int, error) {
	days, ok1 := hs.formPositiveInt(req, "days")
	hours, ok2 := hs.formPositiveInt(req, "hours")
	minutes, ok3 := hs.formPositiveInt(req, "minutes")
	seconds, ok4 := hs.formPositiveInt(req, "seconds")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return 0, errors.New("invalid days, hours, minutes or seconds")
	}
	return days*86400 + hours*3600 + minutes*60 + seconds, nil
}

func (hs *HttpService) formCircleId(req *http.Request, key string) (int, error) { // nolint:golint
	circleId, err := strconv.Atoi(req.FormValue(key)) // nolint:golint
	if err != nil || circleId < 0 || circleId >= len(hs.ip.Circles) {
		return circleId, errors.New("invalid " + key)
	}
	return circleId, nil
}

func (hs *HttpService) formBool(req *http.Request, key string) (bool, error) {
	return strconv.ParseBool(req.FormValue(key))
}

func (hs *HttpService) setWorker(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("worker"))
	if str != "" {
		worker, err := strconv.Atoi(str)
		if err != nil || worker <= 0 || worker > 4*runtime.NumCPU() {
			return errors.New("invalid worker, not more than 4*cpus")
		}
		hs.tx.Worker = worker
	} else {
		hs.tx.Worker = 1
	}
	return nil
}

func (hs *HttpService) setHaAddrs(req *http.Request) error {
	haAddrs := hs.formValues(req, "ha_addrs")
	if len(haAddrs) > 1 {
		r, _ := regexp.Compile(`^[\w-.]+:\d{1,5}$`)
		for _, addr := range haAddrs {
			if !r.MatchString(addr) {
				return errors.New("invalid ha_addrs")
			}
		}
		hs.tx.HaAddrs = haAddrs
	} else if len(haAddrs) == 1 {
		return errors.New("ha_addrs should contain two addrs at least")
	}
	return nil
}
