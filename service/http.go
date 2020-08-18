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

var (
	ErrInvalidSeconds = errors.New("invalid days, hours, minutes or seconds")
	ErrInvalidWorker  = errors.New("invalid worker, not more than 4*cpus")
	ErrInvalidHaAddrs = errors.New("invalid ha_addrs")
	ErrHaAddrsLessTwo = errors.New("ha_addrs requires two addresses at least")
)

type HttpService struct { // nolint:golint
	ip           *backend.Proxy
	tx           *transfer.Transfer
	Username     string
	Password     string
	AuthSecure   bool
	WriteTracing bool
	QueryTracing bool
}

func NewHttpService(cfg *backend.ProxyConfig) (hs *HttpService) { // nolint:golint
	ip := backend.NewProxy(cfg)
	hs = &HttpService{
		ip:           ip,
		tx:           transfer.NewTransfer(cfg, ip.Circles),
		Username:     cfg.Username,
		Password:     cfg.Password,
		AuthSecure:   cfg.AuthSecure,
		WriteTracing: cfg.WriteTracing,
		QueryTracing: cfg.QueryTracing,
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
	hs.WriteHeader(w, 204)
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	db := req.FormValue("db")
	q := req.FormValue("q")
	body, err := hs.ip.Query(w, req)
	if err != nil {
		log.Printf("query error: %s, query: %s %s %s, client: %s", err, req.Method, db, q, req.RemoteAddr)
		hs.WriteError(w, req, 400, err.Error())
		return
	}
	hs.WriteBody(w, body)
	if hs.QueryTracing {
		log.Printf("query: %s %s %s, client: %s", req.Method, db, q, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	if precision == "" {
		precision = "ns"
	}
	db := req.URL.Query().Get("db")
	if db == "" {
		hs.WriteError(w, req, 400, "database not found")
		return
	}
	if len(hs.ip.DBSet) > 0 && !hs.ip.DBSet[db] {
		hs.WriteError(w, req, 400, fmt.Sprintf("database forbidden: %s", db))
		return
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			hs.WriteError(w, req, 400, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	err = hs.ip.Write(p, db, precision)
	if err == nil {
		hs.WriteHeader(w, 204)
	}
	if hs.WriteTracing {
		log.Printf("write: %s %s %s, client: %s", db, precision, p, req.RemoteAddr)
	}
}

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}
	hs.Write(w, req, 200, hs.ip.GetHealth())
}

func (hs *HttpService) HandlerReplica(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	db := req.FormValue("db")
	meas := req.FormValue("meas")
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
		hs.Write(w, req, 200, data)
	} else {
		hs.WriteError(w, req, 400, "invalid db or meas")
	}
}

func (hs *HttpService) HandlerEncrypt(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	msg := req.URL.Query().Get("msg")
	encrypt := util.AesEncrypt(msg)
	hs.WriteText(w, 200, encrypt)
}

func (hs *HttpService) HandlerDencrypt(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	key := req.URL.Query().Get("key")
	msg := req.URL.Query().Get("msg")
	if !util.CheckCipherKey(key) {
		hs.WriteError(w, req, 400, "invalid key")
		return
	}
	decrypt := util.AesDecrypt(msg)
	hs.WriteText(w, 200, decrypt)
}

func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}
	operation := req.FormValue("operation")
	if operation != "add" && operation != "rm" {
		hs.WriteError(w, req, 400, "invalid operation")
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
			hs.WriteError(w, req, 400, "invalid backends from body")
			return
		}
		for _, bkcfg := range body.Backends {
			backends = append(backends, backend.NewSimpleBackend(bkcfg))
			hs.tx.CircleStates[circleId].Stats[bkcfg.Url] = &transfer.Stats{}
		}
	}
	backends = append(backends, hs.ip.Circles[circleId].Backends...)

	if hs.tx.CircleStates[circleId].Transferring {
		hs.WriteText(w, 400, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	dbs := hs.formValues(req, "db")
	go hs.tx.Rebalance(circleId, backends, dbs)
	hs.WriteText(w, 202, "accepted")
}

func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	fromCircleId, err := hs.formCircleId(req, "from_circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}
	toCircleId, err := hs.formCircleId(req, "to_circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}
	if fromCircleId == toCircleId {
		hs.WriteError(w, req, 400, "from_circle_id and to_circle_id cannot be same")
		return
	}

	if hs.tx.CircleStates[fromCircleId].Transferring || hs.tx.CircleStates[toCircleId].Transferring {
		hs.WriteText(w, 400, fmt.Sprintf("circle %d or %d is transferring", fromCircleId, toCircleId))
		return
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	backendUrls := hs.formValues(req, "backend_urls")
	dbs := hs.formValues(req, "db")
	go hs.tx.Recovery(fromCircleId, toCircleId, backendUrls, dbs)
	hs.WriteText(w, 202, "accepted")
}

func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	seconds, err := hs.formSeconds(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	for _, cs := range hs.tx.CircleStates {
		if cs.Transferring {
			hs.WriteText(w, 400, fmt.Sprintf("circle %d is transferring", cs.CircleId))
			return
		}
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	dbs := hs.formValues(req, "db")
	go hs.tx.Resync(dbs, seconds)
	hs.WriteText(w, 202, "accepted")
}

func (hs *HttpService) HandlerCleanup(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	if hs.tx.CircleStates[circleId].Transferring {
		hs.WriteText(w, 400, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.WriteText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	go hs.tx.Cleanup(circleId)
	hs.WriteText(w, 202, "accepted")
}

func (hs *HttpService) HandlerTransferState(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
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
		hs.Write(w, req, 200, state)
		return
	} else if req.Method == "POST" {
		state := make(map[string]interface{})
		if req.FormValue("resyncing") != "" {
			resyncing, err := hs.formBool(req, "resyncing")
			if err != nil {
				hs.WriteError(w, req, 400, "illegal resyncing")
				return
			}
			hs.tx.Resyncing = resyncing
			state["resyncing"] = resyncing
		}
		if req.FormValue("circle_id") != "" || req.FormValue("transferring") != "" {
			circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
			if err != nil {
				hs.WriteError(w, req, 400, err.Error())
				return
			}
			transferring, err := hs.formBool(req, "transferring")
			if err != nil {
				hs.WriteError(w, req, 400, "illegal transferring")
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
			hs.WriteError(w, req, 400, "missing query parameter")
			return
		}
		hs.Write(w, req, 200, state)
		return
	}
}

func (hs *HttpService) HandlerTransferStats(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.WriteError(w, req, 400, err.Error())
		return
	}

	statsType := req.FormValue("type")
	if statsType == "rebalance" || statsType == "recovery" || statsType == "resync" || statsType == "cleanup" {
		hs.Write(w, req, 200, hs.tx.CircleStates[circleId].Stats)
	} else {
		hs.WriteError(w, req, 400, "invalid stats type")
	}
}

func (hs *HttpService) Write(w http.ResponseWriter, req *http.Request, status int, data interface{}) {
	if status >= 400 {
		hs.WriteError(w, req, status, data.(string))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	hs.WriteHeader(w, status)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(data, pretty))
}

func (hs *HttpService) WriteError(w http.ResponseWriter, req *http.Request, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Influxdb-Error", err)
	hs.WriteHeader(w, status)
	rsp := backend.ResponseFromError(err)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(rsp, pretty))
}

func (hs *HttpService) WriteBody(w http.ResponseWriter, body []byte) {
	hs.WriteHeader(w, 200)
	w.Write(body)
}

func (hs *HttpService) WriteText(w http.ResponseWriter, status int, text string) {
	hs.WriteHeader(w, status)
	w.Write([]byte(text + "\n"))
}

func (hs *HttpService) WriteHeader(w http.ResponseWriter, status int) {
	w.Header().Set("X-Influxdb-Version", backend.Version)
	w.WriteHeader(status)
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
	hs.WriteError(w, req, 405, "method not allow")
	return false
}

func (hs *HttpService) checkAuth(w http.ResponseWriter, req *http.Request) bool {
	if hs.Username == "" && hs.Password == "" {
		return true
	}
	u, p := req.URL.Query().Get("u"), req.URL.Query().Get("p")
	if hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
		return true
	}
	u, p, ok := req.BasicAuth()
	if ok && hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
		return true
	}
	hs.WriteError(w, req, 401, "authentication failed")
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
		return 0, ErrInvalidSeconds
	}
	return days*86400 + hours*3600 + minutes*60 + seconds, nil
}

func (hs *HttpService) formCircleId(req *http.Request, key string) (int, error) { // nolint:golint
	circleId, err := strconv.Atoi(req.FormValue(key)) // nolint:golint
	if err != nil || circleId < 0 || circleId >= len(hs.ip.Circles) {
		return circleId, fmt.Errorf("invalid %s", key)
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
			return ErrInvalidWorker
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
				return ErrInvalidHaAddrs
			}
		}
		hs.tx.HaAddrs = haAddrs
	} else if len(haAddrs) == 1 {
		return ErrHaAddrsLessTwo
	}
	return nil
}
