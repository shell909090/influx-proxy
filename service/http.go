package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/transfer"
	"github.com/chengshiwen/influx-proxy/util"
	gzip "github.com/klauspost/pgzip"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

type HttpService struct {
	*backend.Proxy
	*transfer.Transfer
	Username   string
	Password   string
	AuthSecure bool
}

func NewHttpService(cfg *backend.ProxyConfig) (hs *HttpService) {
	ip := backend.NewProxy(cfg)
	hs = &HttpService{
		Proxy:      ip,
		Transfer:   transfer.NewTransfer(cfg, ip.Circles),
		Username:   cfg.Username,
		Password:   cfg.Password,
		AuthSecure: cfg.AuthSecure,
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
	return
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addHeader(w)
	w.WriteHeader(204)
	return
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET", "POST"}) {
		return
	}

	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		w.WriteHeader(400)
		hs.write(w, "query not found")
		return
	}
	if hs.LogEnabled {
		log.Printf("query: %s %s %s, client: %s", req.Method, req.FormValue("db"), q, req.RemoteAddr)
	}

	tokens, check := backend.CheckQuery(q)
	if !check {
		w.WriteHeader(400)
		hs.write(w, "query forbidden")
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
			w.WriteHeader(400)
			hs.write(w, "database not found")
			return
		}
		if hs.CheckForbiddenDB(db) {
			w.WriteHeader(400)
			hs.write(w, fmt.Sprintf("database forbidden: %s", db))
			return
		}
	}

	body, err := hs.Query(w, req, tokens, db, alterDb)
	if err != nil {
		log.Printf("query error: %s, the query is %s, db is %s", err, q, db)
		w.WriteHeader(400)
		hs.writeResp(w, fmt.Sprintf("query error: %s", err))
		return
	}
	w.Write(body)
	return
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
		w.WriteHeader(400)
		hs.write(w, "database not found")
		return
	}
	if hs.CheckForbiddenDB(db) {
		w.WriteHeader(400)
		hs.write(w, fmt.Sprintf("database forbidden: %s", db))
		return
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		defer b.Close()
		if err != nil {
			w.WriteHeader(400)
			hs.write(w, "unable to decode gzip body")
			return
		}
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		w.WriteHeader(400)
		hs.write(w, err.Error())
		return
	}

	err = hs.Write(p, db, precision)
	if err == nil {
		w.WriteHeader(204)
	}
	if hs.LogEnabled {
		log.Printf("write: %s %s %s, client: %s", db, precision, string(p), req.RemoteAddr)
	}
	return
}

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET"}) {
		return
	}

	hs.addJsonHeader(w)
	data := make([]map[string]interface{}, len(hs.Circles))
	for i, c := range hs.Circles {
		data[i] = map[string]interface{}{"circle": c.Name, "backends": c.GetHealth()}
	}
	pretty := req.URL.Query().Get("pretty") == "true"
	res := util.MarshalJson(data, pretty, true)
	w.Write(res)
	return
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
		hs.addJsonHeader(w)
		key := backend.GetKey(db, meas)
		backends := hs.GetBackends(key)
		data := make([]map[string]string, len(backends))
		for i, b := range backends {
			data[i] = map[string]string{"circle": hs.Circles[i].Name, "name": b.Name, "url": b.Url}
		}
		pretty := req.URL.Query().Get("pretty") == "true"
		res := util.MarshalJson(data, pretty, true)
		w.Write(res)
	} else {
		w.WriteHeader(400)
		w.Write([]byte("invalid db or meas\n"))
	}
	return
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
		w.WriteHeader(400)
		w.Write([]byte("invalid key\n"))
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

	circleId, err := hs.formCircleId(req, "circle_id")
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}
	operation := req.FormValue("operation")
	if operation != "add" && operation != "rm" {
		w.WriteHeader(400)
		w.Write([]byte("invalid operation\n"))
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
			w.WriteHeader(400)
			w.Write([]byte("invalid backends from body\n"))
			return
		}
		for _, bkcfg := range body.Backends {
			backends = append(backends, backend.NewSimpleBackend(bkcfg))
			hs.CircleStates[circleId].Stats[bkcfg.Url] = &transfer.Stats{}
		}
	}
	for _, b := range hs.Circles[circleId].Backends {
		backends = append(backends, b)
	}

	if hs.CircleStates[circleId].Transferring {
		w.WriteHeader(202)
		w.Write([]byte(fmt.Sprintf("circle %d is transferring\n", circleId)))
		return
	}
	if hs.Resyncing {
		w.WriteHeader(202)
		w.Write([]byte("proxy is resyncing\n"))
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	dbs := hs.formValues(req, "db")
	go hs.Rebalance(circleId, backends, dbs)
	w.WriteHeader(202)
	w.Write([]byte("accepted\n"))
	return
}

func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	fromCircleId, err := hs.formCircleId(req, "from_circle_id")
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}
	toCircleId, err := hs.formCircleId(req, "to_circle_id")
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}
	if fromCircleId == toCircleId {
		w.WriteHeader(400)
		w.Write([]byte("from_circle_id and to_circle_id cannot be same\n"))
		return
	}

	if hs.CircleStates[fromCircleId].Transferring || hs.CircleStates[toCircleId].Transferring {
		w.WriteHeader(202)
		w.Write([]byte(fmt.Sprintf("circle %d or %d is transferring\n", fromCircleId, toCircleId)))
		return
	}
	if hs.Resyncing {
		w.WriteHeader(202)
		w.Write([]byte("proxy is resyncing\n"))
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	backendUrls := hs.formValues(req, "backend_urls")
	dbs := hs.formValues(req, "db")
	go hs.Recovery(fromCircleId, toCircleId, backendUrls, dbs)
	w.WriteHeader(202)
	w.Write([]byte("accepted\n"))
	return
}

func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	seconds, err := hs.formSeconds(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	for _, cs := range hs.CircleStates {
		if cs.Transferring {
			w.WriteHeader(202)
			w.Write([]byte(fmt.Sprintf("circle %d is transferring\n", cs.CircleId)))
			return
		}
	}
	if hs.Resyncing {
		w.WriteHeader(202)
		w.Write([]byte("proxy is resyncing\n"))
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	dbs := hs.formValues(req, "db")
	go hs.Resync(dbs, seconds)
	w.WriteHeader(202)
	w.Write([]byte("accepted\n"))
	return
}

func (hs *HttpService) HandlerCleanup(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"POST"}) {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id")
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	if hs.CircleStates[circleId].Transferring {
		w.WriteHeader(202)
		w.Write([]byte(fmt.Sprintf("circle %d is transferring\n", circleId)))
		return
	}
	if hs.Resyncing {
		w.WriteHeader(202)
		w.Write([]byte("proxy is resyncing\n"))
		return
	}

	err = hs.setWorker(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	err = hs.setHaAddrs(req)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	go hs.Cleanup(circleId)
	w.WriteHeader(202)
	w.Write([]byte("accepted\n"))
	return
}

func (hs *HttpService) HandlerTransferState(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.addVerHeader(w)
	if !hs.checkMethodAndAuth(w, req, []string{"GET", "POST"}) {
		return
	}

	pretty := req.URL.Query().Get("pretty") == "true"
	if req.Method == "GET" {
		hs.addJsonHeader(w)
		data := make([]map[string]interface{}, len(hs.CircleStates))
		for k, cs := range hs.CircleStates {
			data[k] = map[string]interface{}{
				"circle_id":    cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		state := map[string]interface{}{"resyncing": hs.Resyncing, "circles": data}
		res := util.MarshalJson(state, pretty, true)
		w.Write(res)
		return
	} else if req.Method == "POST" {
		state := make(map[string]interface{})
		if req.FormValue("resyncing") != "" {
			resyncing, err := hs.formBool(req, "resyncing")
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("illegal resyncing\n"))
				return
			}
			hs.Resyncing = resyncing
			state["resyncing"] = hs.Resyncing
		}
		if req.FormValue("circle_id") != "" || req.FormValue("transferring") != "" {
			circleId, err := hs.formCircleId(req, "circle_id")
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error() + "\n"))
				return
			}
			transferring, err := hs.formBool(req, "transferring")
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("illegal transferring\n"))
				return
			}
			cs := hs.CircleStates[circleId]
			cs.Transferring = transferring
			cs.WriteOnly = transferring
			state["circle"] = map[string]interface{}{
				"circle_id":    cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		if len(state) == 0 {
			w.WriteHeader(400)
			w.Write([]byte("missing query parameter\n"))
			return
		}
		hs.addJsonHeader(w)
		res := util.MarshalJson(state, pretty, true)
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

	circleId, err := hs.formCircleId(req, "circle_id")
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error() + "\n"))
		return
	}

	statsType := req.FormValue("type")
	if statsType == "rebalance" || statsType == "recovery" || statsType == "resync" || statsType == "cleanup" {
		hs.addJsonHeader(w)
		pretty := req.URL.Query().Get("pretty") == "true"
		res := util.MarshalJson(hs.CircleStates[circleId].Stats, pretty, true)
		w.Write(res)
	} else {
		w.WriteHeader(400)
		w.Write([]byte("invalid stats type\n"))
	}
	return
}

func (hs *HttpService) addHeader(w http.ResponseWriter) {
	hs.addVerHeader(w)
	hs.addJsonHeader(w)
}

func (hs *HttpService) addVerHeader(w http.ResponseWriter) {
	w.Header().Add("X-Influxdb-Version", backend.VERSION)
}

func (hs *HttpService) addJsonHeader(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "application/json")
}

func (hs *HttpService) write(w http.ResponseWriter, msg string) {
	if hs.LogEnabled {
		log.Printf(msg)
	}
	hs.writeResp(w, msg)
}

func (hs *HttpService) writeResp(w http.ResponseWriter, msg string) {
	if w.Header().Get("Content-Type") == "application/json" {
		rsp := backend.ResponseFromError(msg, true)
		w.Write(util.MarshalJson(rsp, false, true))
	} else {
		w.Write([]byte(msg + "\n"))
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
	w.WriteHeader(405)
	hs.write(w, "method not allow")
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
	w.WriteHeader(401)
	hs.write(w, "authentication failed")
	return false
}

func (hs *HttpService) transAuth(msg string) string {
	if hs.AuthSecure {
		return util.AesEncrypt(msg)
	} else {
		return msg
	}
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

func (hs *HttpService) formCircleId(req *http.Request, key string) (int, error) {
	circleId, err := strconv.Atoi(req.FormValue(key))
	if err != nil || circleId < 0 || circleId >= len(hs.Circles) {
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
		hs.Worker = worker
	} else {
		hs.Worker = 1
	}
	return nil
}

func (hs *HttpService) setHaAddrs(req *http.Request) error {
	haAddrs := hs.formValues(req, "ha_addrs")
	if len(haAddrs) > 1 {
		for _, addr := range haAddrs {
			if match, _ := regexp.MatchString("^[\\w-.]+:\\d{1,5}$", addr); !match {
				return errors.New("invalid ha_addrs")
			}
		}
		hs.HaAddrs = haAddrs
	} else if len(haAddrs) == 1 {
		return errors.New("ha_addrs should contain two addrs at least")
	}
	return nil
}
