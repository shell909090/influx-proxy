// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/shell909090/influx-proxy/monitor"
)

var (
	ErrClosed          = errors.New("write in a closed file")
	ErrBackendNotExist = errors.New("use a backend not exists")
	ErrQueryForbidden  = errors.New("query forbidden")
)

func ScanKey(pointbuf []byte) (key string, err error) {
	var keybuf [100]byte
	keyslice := keybuf[0:0]
	buflen := len(pointbuf)
	for i := 0; i < buflen; i++ {
		c := pointbuf[i]
		switch c {
		case '\\':
			i++
			keyslice = append(keyslice, pointbuf[i])
		case ' ', ',':
			key = string(keyslice)
			return
		default:
			keyslice = append(keyslice, c)
		}
	}
	return "", io.EOF
}

// faster then bytes.TrimRight, not sure why.
func TrimRight(p []byte, s []byte) (r []byte) {
	r = p
	if len(r) == 0 {
		return
	}

	i := len(r) - 1
	for ; bytes.IndexByte(s, r[i]) != -1; i-- {
	}
	return r[0 : i+1]
}

// TODO: kafka next

type InfluxCluster struct {
	lock           sync.RWMutex
	Zone           string
	nexts          string
	query_executor Querier
	ForbiddenQuery []*regexp.Regexp
	ObligatedQuery []*regexp.Regexp
	cfgsrc         *RedisConfigSource
	bas            []BackendAPI
	backends       map[string]BackendAPI
	m2bs           map[string][]BackendAPI // measurements to backends
	stats          *Statistics
	counter        *Statistics
	ticker         *time.Ticker
	defaultTags    map[string]string
	WriteTracing   int
	QueryTracing   int
}

type Statistics struct {
	QueryRequests        int64
	QueryRequestsFail    int64
	WriteRequests        int64
	WriteRequestsFail    int64
	PingRequests         int64
	PingRequestsFail     int64
	PointsWritten        int64
	PointsWrittenFail    int64
	WriteRequestDuration int64
	QueryRequestDuration int64
}

func NewInfluxCluster(cfgsrc *RedisConfigSource, nodecfg *NodeConfig) (ic *InfluxCluster) {
	ic = &InfluxCluster{
		Zone:           nodecfg.Zone,
		nexts:          nodecfg.Nexts,
		query_executor: &InfluxQLExecutor{},
		cfgsrc:         cfgsrc,
		bas:            make([]BackendAPI, 0),
		stats:          &Statistics{},
		counter:        &Statistics{},
		ticker:         time.NewTicker(10 * time.Second),
		defaultTags:    map[string]string{"addr": nodecfg.ListenAddr},
		WriteTracing:   nodecfg.WriteTracing,
		QueryTracing:   nodecfg.QueryTracing,
	}
	host, err := os.Hostname()
	if err != nil {
		log.Println(err)
	}
	ic.defaultTags["host"] = host
	if nodecfg.Interval > 0 {
		ic.ticker = time.NewTicker(time.Second * time.Duration(nodecfg.Interval))
	}

	err = ic.ForbidQuery(ForbidCmds)
	if err != nil {
		panic(err)
		return
	}
	err = ic.EnsureQuery(SupportCmds)
	if err != nil {
		panic(err)
		return
	}

	// feature
	go ic.statistics()
	return
}

func (ic *InfluxCluster) statistics() {
	// how to quit
	for {
		<-ic.ticker.C
		ic.Flush()
		ic.counter = (*Statistics)(atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(&ic.stats)),
			unsafe.Pointer(ic.counter)))
		err := ic.WriteStatistics()
		if err != nil {
			log.Println(err)
		}
	}
}

func (ic *InfluxCluster) Flush() {
	ic.counter.QueryRequests = 0
	ic.counter.QueryRequestsFail = 0
	ic.counter.WriteRequests = 0
	ic.counter.WriteRequestsFail = 0
	ic.counter.PingRequests = 0
	ic.counter.PingRequestsFail = 0
	ic.counter.PointsWritten = 0
	ic.counter.PointsWrittenFail = 0
	ic.counter.WriteRequestDuration = 0
	ic.counter.QueryRequestDuration = 0
}

func (ic *InfluxCluster) WriteStatistics() (err error) {
	metric := &monitor.Metric{
		Name: "influxdb.cluster",
		Tags: ic.defaultTags,
		Fields: map[string]interface{}{
			"statQueryRequest":         ic.counter.QueryRequests,
			"statQueryRequestFail":     ic.counter.QueryRequestsFail,
			"statWriteRequest":         ic.counter.WriteRequests,
			"statWriteRequestFail":     ic.counter.WriteRequestsFail,
			"statPingRequest":          ic.counter.PingRequests,
			"statPingRequestFail":      ic.counter.PingRequestsFail,
			"statPointsWritten":        ic.counter.PointsWritten,
			"statPointsWrittenFail":    ic.counter.PointsWrittenFail,
			"statQueryRequestDuration": ic.counter.QueryRequestDuration,
			"statWriteRequestDuration": ic.counter.WriteRequestDuration,
		},
		Time: time.Now(),
	}
	line, err := metric.ParseToLine()
	if err != nil {
		return
	}
	return ic.Write([]byte(line + "\n"))
}

func (ic *InfluxCluster) ForbidQuery(s string) (err error) {
	r, err := regexp.Compile(s)
	if err != nil {
		return
	}

	ic.lock.Lock()
	defer ic.lock.Unlock()
	ic.ForbiddenQuery = append(ic.ForbiddenQuery, r)
	return
}

func (ic *InfluxCluster) EnsureQuery(s string) (err error) {
	r, err := regexp.Compile(s)
	if err != nil {
		return
	}

	ic.lock.Lock()
	defer ic.lock.Unlock()
	ic.ObligatedQuery = append(ic.ObligatedQuery, r)
	return
}

func (ic *InfluxCluster) AddNext(ba BackendAPI) {
	ic.lock.Lock()
	defer ic.lock.Unlock()
	ic.bas = append(ic.bas, ba)
	return
}

func (ic *InfluxCluster) loadBackends() (backends map[string]BackendAPI, bas []BackendAPI, err error) {
	backends = make(map[string]BackendAPI)

	bkcfgs, err := ic.cfgsrc.LoadBackends()
	if err != nil {
		return
	}

	for name, cfg := range bkcfgs {
		backends[name], err = NewBackends(cfg, name)
		if err != nil {
			log.Printf("create backend error: %s", err)
			return
		}
	}

	if ic.nexts != "" {
		for _, nextname := range strings.Split(ic.nexts, ",") {
			ba, ok := backends[nextname]
			if !ok {
				err = ErrBackendNotExist
				log.Println(nextname, err)
				continue
			}
			bas = append(bas, ba)
		}
	}

	return
}

func (ic *InfluxCluster) loadMeasurements(backends map[string]BackendAPI) (m2bs map[string][]BackendAPI, err error) {
	m2bs = make(map[string][]BackendAPI)

	m_map, err := ic.cfgsrc.LoadMeasurements()
	if err != nil {
		return
	}

	for name, bs_names := range m_map {
		var bss []BackendAPI
		for _, bs_name := range bs_names {
			bs, ok := backends[bs_name]
			if !ok {
				err = ErrBackendNotExist
				log.Println(bs_name, err)
				continue
			}
			bss = append(bss, bs)
		}
		m2bs[name] = bss
	}
	return
}

func (ic *InfluxCluster) LoadConfig() (err error) {
	backends, bas, err := ic.loadBackends()
	if err != nil {
		return
	}

	m2bs, err := ic.loadMeasurements(backends)
	if err != nil {
		return
	}

	ic.lock.Lock()
	orig_backends := ic.backends
	ic.backends = backends
	ic.bas = bas
	ic.m2bs = m2bs
	ic.lock.Unlock()

	for name, bs := range orig_backends {
		err = bs.Close()
		if err != nil {
			log.Printf("fail in close backend %s", name)
		}
	}
	return
}

func (ic *InfluxCluster) Ping() (version string, err error) {
	atomic.AddInt64(&ic.stats.PingRequests, 1)
	version = VERSION
	return
}

func (ic *InfluxCluster) CheckQuery(q string) (err error) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()

	if len(ic.ForbiddenQuery) != 0 {
		for _, fq := range ic.ForbiddenQuery {
			if fq.MatchString(q) {
				return ErrQueryForbidden
			}
		}
	}

	if len(ic.ObligatedQuery) != 0 {
		for _, pq := range ic.ObligatedQuery {
			if pq.MatchString(q) {
				return
			}
		}
		return ErrQueryForbidden
	}

	return
}

func (ic *InfluxCluster) GetBackends(key string) (backends []BackendAPI, ok bool) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()

	backends, ok = ic.m2bs[key]
	// match use prefix
	if !ok {
		for k, v := range ic.m2bs {
			if strings.HasPrefix(key, k) {
				backends = v
				ok = true
				break
			}
		}
	}

	if !ok {
		backends, ok = ic.m2bs["_default_"]
	}

	return
}

func (ic *InfluxCluster) Query(w http.ResponseWriter, req *http.Request) (err error) {
	atomic.AddInt64(&ic.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	switch req.Method {
	case "GET", "POST":
	default:
		w.WriteHeader(400)
		w.Write([]byte("illegal method"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	// TODO: all query in q?
	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		w.WriteHeader(400)
		w.Write([]byte("empty query"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	err = ic.query_executor.Query(w, req)
	if err == nil {
		return
	}

	err = ic.CheckQuery(q)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte("query forbidden"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	key, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		log.Printf("can't get measurement: %s\n", q)
		w.WriteHeader(400)
		w.Write([]byte("can't get measurement"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	apis, ok := ic.GetBackends(key)
	if !ok {
		log.Printf("unknown measurement: %s,the query is %s\n", key, q)
		w.WriteHeader(400)
		w.Write([]byte("unknown measurement"))
		atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
		return
	}

	// same zone first, other zone. pass non-active.
	// TODO: better way?

	for _, api := range apis {
		if api.GetZone() != ic.Zone {
			continue
		}
		if !api.IsActive() || api.IsWriteOnly() {
			continue
		}
		err = api.Query(w, req)
		if err == nil {
			return
		}
	}

	for _, api := range apis {
		if api.GetZone() == ic.Zone {
			continue
		}
		if !api.IsActive() {
			continue
		}
		err = api.Query(w, req)
		if err == nil {
			return
		}
	}

	w.WriteHeader(400)
	w.Write([]byte("query error"))
	atomic.AddInt64(&ic.stats.QueryRequestsFail, 1)
	return
}

// Wrong in one row will not stop others.
// So don't try to return error, just print it.
func (ic *InfluxCluster) WriteRow(line []byte) {
	atomic.AddInt64(&ic.stats.PointsWritten, 1)
	// maybe trim?
	line = bytes.TrimRight(line, " \t\r\n")

	// empty line, ignore it.
	if len(line) == 0 {
		return
	}

	key, err := ScanKey(line)
	if err != nil {
		log.Printf("scan key error: %s\n", err)
		atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
		return
	}

	bs, ok := ic.GetBackends(key)
	if !ok {
		log.Printf("new measurement: %s\n", key)
		atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
		// TODO: new measurement?
		return
	}

	// don't block here for a lont time, we just have one worker.
	for _, b := range bs {
		err = b.Write(line)
		if err != nil {
			log.Printf("cluster write fail: %s\n", key)
			atomic.AddInt64(&ic.stats.PointsWrittenFail, 1)
			return
		}
	}
	return
}

func (ic *InfluxCluster) Write(p []byte) (err error) {
	atomic.AddInt64(&ic.stats.WriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ic.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	buf := bytes.NewBuffer(p)

	var line []byte
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			log.Printf("error: %s\n", err)
			atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
			return
		case io.EOF, nil:
			err = nil
		}

		if len(line) == 0 {
			break
		}

		ic.WriteRow(line)
	}

	ic.lock.RLock()
	defer ic.lock.RUnlock()
	if len(ic.bas) > 0 {
		for _, n := range ic.bas {
			err = n.Write(p)
			if err != nil {
				log.Printf("error: %s\n", err)
				atomic.AddInt64(&ic.stats.WriteRequestsFail, 1)
			}
		}
	}

	return
}

func (ic *InfluxCluster) Close() (err error) {
	ic.lock.RLock()
	defer ic.lock.RUnlock()
	for name, bs := range ic.backends {
		err = bs.Close()
		if err != nil {
			log.Printf("fail in close backend %s", name)
		}
	}
	return
}
