package backend

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/deckarep/golang-set"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Proxy struct {
	Circles      []*Circle
	dbList       []string
	dbSet        mapset.Set
	MlogDir      string
	LogEnabled   bool
	Username     string
	Password     string
	AuthSecure   bool
	HTTPSEnabled bool
	HaAddrs      []string
	IsResyncing  bool
	MigrateCpus  int
	MigrateBatch int
	MigrateStats []map[string]*MigrateInfo
	lock         sync.RWMutex
}

type MigrateInfo struct {
	DatabaseTotal    int32 `json:"database_total"`
	DatabaseDone     int32 `json:"database_done"`
	MeasurementTotal int32 `json:"measurement_total"`
	MeasurementDone  int32 `json:"measurement_done"`
	MigrateCount     int32 `json:"migrate_count"`
	InPlaceCount     int32 `json:"inplace_count"`
}

func NewProxy(cfg *ProxyConfig) (proxy *Proxy) {
	proxy = &Proxy{
		Circles:      make([]*Circle, len(cfg.Circles)),
		dbList:       cfg.DbList,
		dbSet:        util.NewSetFromStrSlice(cfg.DbList),
		MlogDir:      cfg.MlogDir,
		LogEnabled:   cfg.LogEnabled,
		Username:     cfg.Username,
		Password:     cfg.Password,
		AuthSecure:   cfg.AuthSecure,
		HTTPSEnabled: cfg.HTTPSEnabled,
	}
	proxy.MigrateStats = make([]map[string]*MigrateInfo, len(cfg.Circles))
	for circleId, circfg := range cfg.Circles {
		proxy.Circles[circleId] = NewCircle(circfg, cfg, circleId)
		proxy.MigrateStats[circleId] = make(map[string]*MigrateInfo)
		for _, bkcfg := range circfg.Backends {
			proxy.MigrateStats[circleId][bkcfg.Url] = &MigrateInfo{}
		}
	}
	if proxy.MigrateCpus <= 0 {
		proxy.MigrateCpus = 1
	}
	if proxy.MigrateBatch <= 0 {
		proxy.MigrateBatch = 25000
	}
	return
}

func GetKey(db, meas string) string {
	var b strings.Builder
	b.Grow(len(db) + len(meas) + 1)
	b.WriteString(db)
	b.WriteString(",")
	b.WriteString(meas)
	return b.String()
}

func (proxy *Proxy) GetBackends(key string) []*Backend {
	backends := make([]*Backend, len(proxy.Circles))
	for i, circle := range proxy.Circles {
		backend := circle.GetBackend(key)
		backends[i] = backend
	}
	return backends
}

func (proxy *Proxy) Query(w http.ResponseWriter, req *http.Request, tokens []string, db string, alterDb bool) (body []byte, err error) {
	if CheckSelectOrShowFromTokens(tokens) {
		var circle *Circle
		badIds := mapset.NewSet()
		for {
			id := rand.Intn(len(proxy.Circles))
			if badIds.Contains(id) {
				continue
			}
			circle = proxy.Circles[id]
			if circle.IsMigrating {
				badIds.Add(id)
				continue
			}
			if circle.CheckStatus() {
				break
			}
			badIds.Add(id)
			if badIds.Cardinality() == len(proxy.Circles) {
				return nil, errors.New("circles unavailable")
			}
			time.Sleep(time.Microsecond)
		}
		meas, err := GetMeasurementFromTokens(tokens)
		if err == nil {
			// available circle -> key(db,meas) -> backend -> select or show
			key := GetKey(db, meas)
			backend := circle.GetBackend(key)
			proxy.Logf("query circle: %d backend: %s", circle.CircleId, backend.Url)
			return backend.Query(req, w, false)
		} else {
			// available circle -> all backends -> show
			proxy.Logf("query circle: %d", circle.CircleId)
			return circle.Query(w, req, tokens)
		}
	} else if CheckDeleteOrDropMeasurementFromTokens(tokens) {
		// all circles -> key(db,meas) -> backend -> delete or drop
		meas, err := GetMeasurementFromTokens(tokens)
		if err != nil {
			return nil, err
		}
		var reqBodyBytes []byte
		if req.Body != nil {
			reqBodyBytes, _ = ioutil.ReadAll(req.Body)
		}
		key := GetKey(db, meas)
		backends := proxy.GetBackends(key)
		for _, backend := range backends {
			proxy.Logf("query backend: %s", backend.Url)
			req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
			body, err = backend.Query(req, w, false)
			if err != nil {
				return nil, err
			}
		}
	} else if alterDb {
		// all circles -> all backends -> create or drop database
		for _, circle := range proxy.Circles {
			if !circle.CheckStatus() {
				return nil, errors.New(fmt.Sprintf("circle %d not health", circle.CircleId))
			}
		}
		for _, circle := range proxy.Circles {
			proxy.Logf("query circle: %d", circle.CircleId)
			body, err = circle.Query(w, req, tokens)
			if err != nil {
				return
			}
		}
	}
	return
}

func (proxy *Proxy) Write(p []byte, db, precision string) (err error) {
	buf := bytes.NewBuffer(p)
	var line []byte
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			log.Printf("error: %s\n", err)
			return
		case io.EOF, nil:
			err = nil
		}
		if len(line) == 0 {
			break
		}
		proxy.WriteRow(line, db, precision)
	}
	return
}

func (proxy *Proxy) WriteRow(line []byte, db, precision string) {
	nanoLine := AppendNano(line, precision)
	meas, err := ScanKey(nanoLine)
	if err != nil {
		log.Printf("scan key error: %s", err)
		return
	}
	if !RapidCheck(nanoLine[len(meas):]) {
		log.Printf("invalid format, drop data: %s %s %s", db, precision, string(line))
		return
	}

	key := GetKey(db, meas)
	backends := proxy.GetBackends(key)
	if len(backends) == 0 {
		log.Printf("write data error: get backends return 0")
		return
	}

	point := &LinePoint{db, nanoLine}
	for _, backend := range backends {
		err := backend.WritePoint(point)
		if err != nil {
			log.Printf("write data to buffer error: %s, %s, %s, %s, %s", err, backend.Url, db, precision, string(line))
		}
	}
}

func (proxy *Proxy) CheckForbiddenDB(db string) bool {
	return len(proxy.dbList) > 0 && !proxy.dbSet.Contains(db)
}

func (proxy *Proxy) Logf(format string, v ...interface{}) {
	if proxy.LogEnabled {
		log.Printf(format, v...)
	}
}

func (proxy *Proxy) GetDatabases() []string {
	for _, circle := range proxy.Circles {
		for _, backend := range circle.Backends {
			if backend.Active {
				return backend.GetDatabases()
			}
		}
	}
	return nil
}

func (proxy *Proxy) GetBackendUrls(backends []*Backend) []string {
	backendUrls := make([]string, len(backends))
	for k, b := range backends {
		backendUrls[k] = b.Url
	}
	return backendUrls
}

func (proxy *Proxy) Migrate(backend *Backend, dstBackends []*Backend, circle *Circle, db, meas string, seconds int) {
	err := circle.Migrate(backend, dstBackends, db, meas, seconds, proxy.MigrateBatch)
	if err != nil {
		util.Mlog.Printf("migrate error:%s src:%s dst:%v circle:%d db:%s measurement:%s seconds:%d", err, backend.Url, proxy.GetBackendUrls(dstBackends), circle.CircleId, db, meas, seconds)
	}
	defer circle.BackendWgMap[backend.Url].Done()
}

func (proxy *Proxy) Rebalance(circleId int, backends []*Backend, databases []string) {
	util.SetMLog(proxy.MlogDir, "rebalance.log")
	util.Mlog.Printf("rebalance start")
	circle := proxy.Circles[circleId]
	proxy.SetMigratingAndBroadcast(circle, true)
	if len(databases) == 0 {
		databases = proxy.GetDatabases()
	}
	proxy.ClearMigrateStats()
	for _, backend := range backends {
		circle.MigrateWg.Add(1)
		go proxy.RebalanceBackend(backend, circle, databases)
	}
	circle.MigrateWg.Wait()
	defer proxy.SetMigratingAndBroadcast(circle, false)
	util.Mlog.Printf("rebalance done")
}

func (proxy *Proxy) RebalanceBackend(backend *Backend, circle *Circle, databases []string) {
	var migrateCount int
	defer circle.MigrateWg.Done()
	circleId := circle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	stats := proxy.MigrateStats[circleId][backend.Url]
	stats.DatabaseTotal = int32(len(databases))
	measuresOfDbs := make([][]string, len(databases))
	for i, db := range databases {
		measuresOfDbs[i] = backend.GetMeasurements(db)
		stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
	}

	for i, db := range databases {
		for j, meas := range measuresOfDbs[i] {
			key := GetKey(db, meas)
			dstBackend := circle.GetBackend(key)
			if dstBackend.Url != backend.Url {
				util.Mlog.Printf("src:%s dst:%s db:%s measurement:%s", backend.Url, dstBackend.Url, db, meas)
				migrateCount++
				circle.BackendWgMap[backend.Url].Add(1)
				go proxy.Migrate(backend, []*Backend{dstBackend}, circle, db, meas, 0)
				if migrateCount%proxy.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
					circle.BackendWgMap[backend.Url].Wait()
				}
				atomic.AddInt32(&stats.MigrateCount, 1)
			} else {
				atomic.AddInt32(&stats.InPlaceCount, 1)
			}
			atomic.AddInt32(&stats.MeasurementDone, 1)
		}
		atomic.AddInt32(&stats.DatabaseDone, 1)
	}
}

func (proxy *Proxy) Recovery(fromCircleId, toCircleId int, recoveryUrls []string, databases []string) {
	util.SetMLog(proxy.MlogDir, "recovery.log")
	util.Mlog.Printf("recovery start")
	fromCircle := proxy.Circles[fromCircleId]
	toCircle := proxy.Circles[toCircleId]

	proxy.SetMigratingAndBroadcast(toCircle, true)
	if len(databases) == 0 {
		databases = proxy.GetDatabases()
	}
	recoveryUrlSet := mapset.NewSet()
	if len(recoveryUrls) != 0 {
		for _, u := range recoveryUrls {
			recoveryUrlSet.Add(u)
		}
	} else {
		for _, b := range toCircle.Backends {
			recoveryUrlSet.Add(b.Url)
		}
	}
	proxy.ClearMigrateStats()
	for _, backend := range fromCircle.Backends {
		fromCircle.MigrateWg.Add(1)
		go proxy.RecoveryBackend(backend, fromCircle, toCircle, recoveryUrlSet, databases)
	}
	fromCircle.MigrateWg.Wait()
	defer proxy.SetMigratingAndBroadcast(toCircle, false)
	util.Mlog.Printf("recovery done")
}

func (proxy *Proxy) RecoveryBackend(backend *Backend, fromCircle, toCircle *Circle, recoveryUrlSet mapset.Set, databases []string) {
	var migrateCount int
	defer fromCircle.MigrateWg.Done()
	fromCircleId := fromCircle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	stats := proxy.MigrateStats[fromCircleId][backend.Url]
	stats.DatabaseTotal = int32(len(databases))
	measuresOfDbs := make([][]string, len(databases))
	for i, db := range databases {
		measuresOfDbs[i] = backend.GetMeasurements(db)
		stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
	}

	for i, db := range databases {
		for j, meas := range measuresOfDbs[i] {
			key := GetKey(db, meas)
			dstBackend := toCircle.GetBackend(key)
			if recoveryUrlSet.Contains(dstBackend.Url) {
				util.Mlog.Printf("src:%s dst:%s db:%s measurement:%s", backend.Url, dstBackend.Url, db, meas)
				migrateCount++
				fromCircle.BackendWgMap[backend.Url].Add(1)
				go proxy.Migrate(backend, []*Backend{dstBackend}, fromCircle, db, meas, 0)
				if migrateCount%proxy.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
					fromCircle.BackendWgMap[backend.Url].Wait()
				}
				atomic.AddInt32(&stats.MigrateCount, 1)
			} else {
				atomic.AddInt32(&stats.InPlaceCount, 1)
			}
			atomic.AddInt32(&stats.MeasurementDone, 1)
		}
		atomic.AddInt32(&stats.DatabaseDone, 1)
	}
}

func (proxy *Proxy) Resync(databases []string, seconds int) {
	util.SetMLog(proxy.MlogDir, "resync.log")
	util.Mlog.Printf("resync start")
	if len(databases) == 0 {
		databases = proxy.GetDatabases()
	}
	proxy.SetResyncingAndBroadcast(true)
	proxy.ClearMigrateStats()
	for _, circle := range proxy.Circles {
		for _, backend := range circle.Backends {
			circle.MigrateWg.Add(1)
			go proxy.ResyncBackend(backend, circle, databases, seconds)
		}
		circle.MigrateWg.Wait()
		util.Mlog.Printf("circle %d resync done", circle.CircleId)
	}
	defer proxy.SetResyncingAndBroadcast(false)
	util.Mlog.Printf("resync done")
}

func (proxy *Proxy) ResyncBackend(backend *Backend, circle *Circle, databases []string, seconds int) {
	var migrateCount int
	defer circle.MigrateWg.Done()
	circleId := circle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	stats := proxy.MigrateStats[circleId][backend.Url]
	stats.DatabaseTotal = int32(len(databases))
	measuresOfDbs := make([][]string, len(databases))
	for i, db := range databases {
		measuresOfDbs[i] = backend.GetMeasurements(db)
		stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
	}

	for i, db := range databases {
		for j, meas := range measuresOfDbs[i] {
			key := GetKey(db, meas)
			dstBackends := make([]*Backend, 0)
			for _, toCircle := range proxy.Circles {
				if toCircle.CircleId != circleId {
					dstBackend := toCircle.GetBackend(key)
					dstBackends = append(dstBackends, dstBackend)
				}
			}
			if len(dstBackends) > 0 {
				util.Mlog.Printf("src:%s dst:%v db:%s measurement:%s", backend.Url, proxy.GetBackendUrls(dstBackends), db, meas)
				migrateCount++
				circle.BackendWgMap[backend.Url].Add(1)
				go proxy.Migrate(backend, dstBackends, circle, db, meas, seconds)
				if migrateCount%proxy.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
					circle.BackendWgMap[backend.Url].Wait()
				}
				atomic.AddInt32(&stats.MigrateCount, 1)
			} else {
				atomic.AddInt32(&stats.InPlaceCount, 1)
			}
			atomic.AddInt32(&stats.MeasurementDone, 1)
		}
		atomic.AddInt32(&stats.DatabaseDone, 1)
	}
}

func (proxy *Proxy) Clear(circleId int) {
	util.SetMLog(proxy.MlogDir, "clear.log")
	util.Mlog.Printf("clear start")
	circle := proxy.Circles[circleId]
	proxy.SetMigratingAndBroadcast(circle, true)
	proxy.ClearMigrateStats()
	for _, backend := range circle.Backends {
		circle.MigrateWg.Add(1)
		go proxy.ClearBackend(backend, circle)
	}
	circle.MigrateWg.Wait()
	defer proxy.SetMigratingAndBroadcast(circle, false)
	util.Mlog.Printf("clear done")
}

func (proxy *Proxy) ClearBackend(backend *Backend, circle *Circle) {
	var migrateCount int
	defer circle.MigrateWg.Done()
	circleId := circle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	databases := backend.GetDatabases()
	stats := proxy.MigrateStats[circleId][backend.Url]
	stats.DatabaseTotal = int32(len(databases))
	measuresOfDbs := make([][]string, len(databases))
	for i, db := range databases {
		measuresOfDbs[i] = backend.GetMeasurements(db)
		stats.MeasurementTotal += int32(len(measuresOfDbs[i]))
	}

	for i, db := range databases {
		for j, meas := range measuresOfDbs[i] {
			util.Mlog.Printf("check backend:%s db:%s measurement:%s", backend.Url, db, meas)
			key := GetKey(db, meas)
			dstBackend := circle.GetBackend(key)
			if dstBackend.Url != backend.Url {
				util.Mlog.Printf("backend:%s db:%s measurement:%s should migrate to %s", backend.Url, db, meas, dstBackend.Url)
				migrateCount++
				circle.BackendWgMap[backend.Url].Add(1)
				go func(backend *Backend, circle *Circle, db, meas string) {
					_, err := backend.DropMeasurement(db, meas)
					if err == nil {
						util.Mlog.Printf("clear backend:%s db:%s measurement:%s done", backend.Url, db, meas)
					} else {
						util.Mlog.Printf("clear backend:%s db:%s measurement:%s error: %s", backend.Url, db, meas, err)
					}
					defer circle.BackendWgMap[backend.Url].Done()
				}(backend, circle, db, meas)
				if migrateCount%proxy.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
					circle.BackendWgMap[backend.Url].Wait()
				}
				atomic.AddInt32(&stats.MigrateCount, 1)
			} else {
				atomic.AddInt32(&stats.InPlaceCount, 1)
			}
			atomic.AddInt32(&stats.MeasurementDone, 1)
		}
		atomic.AddInt32(&stats.DatabaseDone, 1)
	}
}

func (proxy *Proxy) ClearMigrateStats() {
	for _, stats := range proxy.MigrateStats {
		for _, mi := range stats {
			mi.DatabaseTotal = 0
			mi.DatabaseDone = 0
			mi.MeasurementTotal = 0
			mi.MeasurementDone = 0
			mi.MigrateCount = 0
			mi.InPlaceCount = 0
		}
	}
}

func (proxy *Proxy) SetResyncing(resyncing bool) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	proxy.IsResyncing = resyncing
}

func (proxy *Proxy) SetMigrating(circle *Circle, migrating bool) {
	proxy.lock.Lock()
	defer proxy.lock.Unlock()
	circle.IsMigrating = migrating
}

func (proxy *Proxy) SetResyncingAndBroadcast(resyncing bool) {
	proxy.SetResyncing(resyncing)
	client := NewClient(proxy.HTTPSEnabled, 10)
	for _, addr := range proxy.HaAddrs {
		url := fmt.Sprintf("http://%s/migrate/state?resyncing=%t", addr, resyncing)
		proxy.PostBroadcast(client, url)
	}
}

func (proxy *Proxy) SetMigratingAndBroadcast(circle *Circle, migrating bool) {
	proxy.SetMigrating(circle, migrating)
	client := NewClient(proxy.HTTPSEnabled, 10)
	for _, addr := range proxy.HaAddrs {
		url := fmt.Sprintf("http://%s/migrate/state?circle_id=%d&migrating=%t", addr, circle.CircleId, migrating)
		proxy.PostBroadcast(client, url)
	}
}

func (proxy *Proxy) PostBroadcast(client *http.Client, url string) {
	if proxy.HTTPSEnabled {
		url = strings.Replace(url, "http", "https", 1)
	}
	req, _ := http.NewRequest("POST", url, nil)
	if proxy.Username != "" || proxy.Password != "" {
		SetBasicAuth(req, proxy.Username, proxy.Password, proxy.AuthSecure)
	}
	client.Do(req)
}
