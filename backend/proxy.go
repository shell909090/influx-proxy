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

func NewProxy(cfg *ProxyConfig) (ip *Proxy) {
	ip = &Proxy{
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
	ip.MigrateStats = make([]map[string]*MigrateInfo, len(cfg.Circles))
	for idx, circfg := range cfg.Circles {
		ip.Circles[idx] = NewCircle(circfg, cfg, idx)
		ip.MigrateStats[idx] = make(map[string]*MigrateInfo)
		for _, bkcfg := range circfg.Backends {
			ip.MigrateStats[idx][bkcfg.Url] = &MigrateInfo{}
		}
	}
	if ip.MigrateCpus <= 0 {
		ip.MigrateCpus = 1
	}
	if ip.MigrateBatch <= 0 {
		ip.MigrateBatch = 25000
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

func (ip *Proxy) GetBackends(key string) []*Backend {
	backends := make([]*Backend, len(ip.Circles))
	for i, circle := range ip.Circles {
		backend := circle.GetBackend(key)
		backends[i] = backend
	}
	return backends
}

func (ip *Proxy) Query(w http.ResponseWriter, req *http.Request, tokens []string, db string, alterDb bool) (body []byte, err error) {
	if CheckSelectOrShowFromTokens(tokens) {
		var circle *Circle
		badIds := mapset.NewSet()
		for {
			id := rand.Intn(len(ip.Circles))
			if badIds.Contains(id) {
				continue
			}
			circle = ip.Circles[id]
			if circle.IsMigrating {
				badIds.Add(id)
				continue
			}
			if circle.CheckStatus() {
				break
			}
			badIds.Add(id)
			if badIds.Cardinality() == len(ip.Circles) {
				return nil, errors.New("circles unavailable")
			}
			time.Sleep(time.Microsecond)
		}
		meas, err := GetMeasurementFromTokens(tokens)
		if err == nil {
			// available circle -> key(db,meas) -> backend -> select or show
			key := GetKey(db, meas)
			backend := circle.GetBackend(key)
			ip.Logf("query circle: %d backend: %s", circle.CircleId, backend.Url)
			return backend.Query(req, w, false)
		} else {
			// available circle -> all backends -> show
			ip.Logf("query circle: %d", circle.CircleId)
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
		backends := ip.GetBackends(key)
		for _, backend := range backends {
			ip.Logf("query backend: %s", backend.Url)
			req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
			body, err = backend.Query(req, w, false)
			if err != nil {
				return nil, err
			}
		}
	} else if alterDb {
		// all circles -> all backends -> create or drop database
		for _, circle := range ip.Circles {
			if !circle.CheckStatus() {
				return nil, errors.New(fmt.Sprintf("circle %d not health", circle.CircleId))
			}
		}
		for _, circle := range ip.Circles {
			ip.Logf("query circle: %d", circle.CircleId)
			body, err = circle.Query(w, req, tokens)
			if err != nil {
				return
			}
		}
	}
	return
}

func (ip *Proxy) Write(p []byte, db, precision string) (err error) {
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
		ip.WriteRow(line, db, precision)
	}
	return
}

func (ip *Proxy) WriteRow(line []byte, db, precision string) {
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
	backends := ip.GetBackends(key)
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

func (ip *Proxy) CheckForbiddenDB(db string) bool {
	return len(ip.dbList) > 0 && !ip.dbSet.Contains(db)
}

func (ip *Proxy) Logf(format string, v ...interface{}) {
	if ip.LogEnabled {
		log.Printf(format, v...)
	}
}

func (ip *Proxy) GetDatabases() []string {
	for _, circle := range ip.Circles {
		for _, backend := range circle.Backends {
			if backend.Active {
				return backend.GetDatabases()
			}
		}
	}
	return nil
}

func (ip *Proxy) GetBackendUrls(backends []*Backend) []string {
	backendUrls := make([]string, len(backends))
	for k, b := range backends {
		backendUrls[k] = b.Url
	}
	return backendUrls
}

func (ip *Proxy) Migrate(backend *Backend, dstBackends []*Backend, circle *Circle, db, meas string, seconds int) {
	err := circle.Migrate(backend, dstBackends, db, meas, seconds, ip.MigrateBatch)
	if err != nil {
		util.Mlog.Printf("migrate error:%s src:%s dst:%v circle:%d db:%s measurement:%s seconds:%d", err, backend.Url, ip.GetBackendUrls(dstBackends), circle.CircleId, db, meas, seconds)
	}
	defer circle.BackendWgMap[backend.Url].Done()
}

func (ip *Proxy) Rebalance(circleId int, backends []*Backend, databases []string) {
	util.SetMLog(ip.MlogDir, "rebalance.log")
	util.Mlog.Printf("rebalance start")
	circle := ip.Circles[circleId]
	ip.SetMigratingAndBroadcast(circle, true)
	if len(databases) == 0 {
		databases = ip.GetDatabases()
	}
	ip.ClearMigrateStats()
	for _, backend := range backends {
		circle.MigrateWg.Add(1)
		go ip.RebalanceBackend(backend, circle, databases)
	}
	circle.MigrateWg.Wait()
	defer ip.SetMigratingAndBroadcast(circle, false)
	util.Mlog.Printf("rebalance done")
}

func (ip *Proxy) RebalanceBackend(backend *Backend, circle *Circle, databases []string) {
	var migrateCount int
	defer circle.MigrateWg.Done()
	circleId := circle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	stats := ip.MigrateStats[circleId][backend.Url]
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
				go ip.Migrate(backend, []*Backend{dstBackend}, circle, db, meas, 0)
				if migrateCount%ip.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
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

func (ip *Proxy) Recovery(fromCircleId, toCircleId int, recoveryUrls []string, databases []string) {
	util.SetMLog(ip.MlogDir, "recovery.log")
	util.Mlog.Printf("recovery start")
	fromCircle := ip.Circles[fromCircleId]
	toCircle := ip.Circles[toCircleId]

	ip.SetMigratingAndBroadcast(toCircle, true)
	if len(databases) == 0 {
		databases = ip.GetDatabases()
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
	ip.ClearMigrateStats()
	for _, backend := range fromCircle.Backends {
		fromCircle.MigrateWg.Add(1)
		go ip.RecoveryBackend(backend, fromCircle, toCircle, recoveryUrlSet, databases)
	}
	fromCircle.MigrateWg.Wait()
	defer ip.SetMigratingAndBroadcast(toCircle, false)
	util.Mlog.Printf("recovery done")
}

func (ip *Proxy) RecoveryBackend(backend *Backend, fromCircle, toCircle *Circle, recoveryUrlSet mapset.Set, databases []string) {
	var migrateCount int
	defer fromCircle.MigrateWg.Done()
	fromCircleId := fromCircle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	stats := ip.MigrateStats[fromCircleId][backend.Url]
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
				go ip.Migrate(backend, []*Backend{dstBackend}, fromCircle, db, meas, 0)
				if migrateCount%ip.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
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

func (ip *Proxy) Resync(databases []string, seconds int) {
	util.SetMLog(ip.MlogDir, "resync.log")
	util.Mlog.Printf("resync start")
	if len(databases) == 0 {
		databases = ip.GetDatabases()
	}
	ip.SetResyncingAndBroadcast(true)
	ip.ClearMigrateStats()
	for _, circle := range ip.Circles {
		for _, backend := range circle.Backends {
			circle.MigrateWg.Add(1)
			go ip.ResyncBackend(backend, circle, databases, seconds)
		}
		circle.MigrateWg.Wait()
		util.Mlog.Printf("circle %d resync done", circle.CircleId)
	}
	defer ip.SetResyncingAndBroadcast(false)
	util.Mlog.Printf("resync done")
}

func (ip *Proxy) ResyncBackend(backend *Backend, circle *Circle, databases []string, seconds int) {
	var migrateCount int
	defer circle.MigrateWg.Done()
	circleId := circle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	stats := ip.MigrateStats[circleId][backend.Url]
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
			for _, toCircle := range ip.Circles {
				if toCircle.CircleId != circleId {
					dstBackend := toCircle.GetBackend(key)
					dstBackends = append(dstBackends, dstBackend)
				}
			}
			if len(dstBackends) > 0 {
				util.Mlog.Printf("src:%s dst:%v db:%s measurement:%s", backend.Url, ip.GetBackendUrls(dstBackends), db, meas)
				migrateCount++
				circle.BackendWgMap[backend.Url].Add(1)
				go ip.Migrate(backend, dstBackends, circle, db, meas, seconds)
				if migrateCount%ip.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
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

func (ip *Proxy) Clear(circleId int) {
	util.SetMLog(ip.MlogDir, "clear.log")
	util.Mlog.Printf("clear start")
	circle := ip.Circles[circleId]
	ip.SetMigratingAndBroadcast(circle, true)
	ip.ClearMigrateStats()
	for _, backend := range circle.Backends {
		circle.MigrateWg.Add(1)
		go ip.ClearBackend(backend, circle)
	}
	circle.MigrateWg.Wait()
	defer ip.SetMigratingAndBroadcast(circle, false)
	util.Mlog.Printf("clear done")
}

func (ip *Proxy) ClearBackend(backend *Backend, circle *Circle) {
	var migrateCount int
	defer circle.MigrateWg.Done()
	circleId := circle.CircleId
	if !backend.Active {
		util.Mlog.Printf("backend not active: %s", backend.Url)
		return
	}

	databases := backend.GetDatabases()
	stats := ip.MigrateStats[circleId][backend.Url]
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
				if migrateCount%ip.MigrateCpus == 0 || (i+1 == len(databases) && j+1 == len(measuresOfDbs[i])) {
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

func (ip *Proxy) ClearMigrateStats() {
	for _, stats := range ip.MigrateStats {
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

func (ip *Proxy) SetResyncing(resyncing bool) {
	ip.lock.Lock()
	defer ip.lock.Unlock()
	ip.IsResyncing = resyncing
}

func (ip *Proxy) SetMigrating(circle *Circle, migrating bool) {
	ip.lock.Lock()
	defer ip.lock.Unlock()
	circle.IsMigrating = migrating
}

func (ip *Proxy) SetResyncingAndBroadcast(resyncing bool) {
	ip.SetResyncing(resyncing)
	client := NewClient(ip.HTTPSEnabled, 10)
	for _, addr := range ip.HaAddrs {
		url := fmt.Sprintf("http://%s/migrate/state?resyncing=%t", addr, resyncing)
		ip.PostBroadcast(client, url)
	}
}

func (ip *Proxy) SetMigratingAndBroadcast(circle *Circle, migrating bool) {
	ip.SetMigrating(circle, migrating)
	client := NewClient(ip.HTTPSEnabled, 10)
	for _, addr := range ip.HaAddrs {
		url := fmt.Sprintf("http://%s/migrate/state?circle_id=%d&migrating=%t", addr, circle.CircleId, migrating)
		ip.PostBroadcast(client, url)
	}
}

func (ip *Proxy) PostBroadcast(client *http.Client, url string) {
	if ip.HTTPSEnabled {
		url = strings.Replace(url, "http", "https", 1)
	}
	req, _ := http.NewRequest("POST", url, nil)
	if ip.Username != "" || ip.Password != "" {
		SetBasicAuth(req, ip.Username, ip.Password, ip.AuthSecure)
	}
	client.Do(req)
}
