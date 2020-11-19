package backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chengshiwen/influx-proxy/util"
)

var (
	ErrEmptyQuery          = errors.New("empty query")
	ErrDatabaseNotFound    = errors.New("database not found")
	ErrBackendsUnavailable = errors.New("backends unavailable")
	ErrGetMeasurement      = errors.New("can't get measurement")
	ErrGetBackends         = errors.New("can't get backends")
)

type Proxy struct {
	Circles []*Circle
	DBSet   util.Set
}

func NewProxy(cfg *ProxyConfig) (ip *Proxy) {
	ip = &Proxy{
		Circles: make([]*Circle, len(cfg.Circles)),
		DBSet:   util.NewSet(),
	}
	for idx, circfg := range cfg.Circles {
		ip.Circles[idx] = NewCircle(circfg, cfg, idx)
	}
	for _, db := range cfg.DBList {
		ip.DBSet.Add(db)
	}
	rand.Seed(time.Now().Unix())
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
		backends[i] = circle.GetBackend(key)
	}
	return backends
}

func (ip *Proxy) GetHealth(stats bool) []interface{} {
	var wg sync.WaitGroup
	health := make([]interface{}, len(ip.Circles))
	for i, c := range ip.Circles {
		wg.Add(1)
		go func(i int, c *Circle) {
			defer wg.Done()
			health[i] = c.GetHealth(stats)
		}(i, c)
	}
	wg.Wait()
	return health
}

func (ip *Proxy) optimalCircle() (c *Circle) {
	actives := make([]int, len(ip.Circles))
	for i, c := range ip.Circles {
		actives[i] = c.GetActiveCount()
	}
	maxActive := actives[0]
	c = ip.Circles[0]
	for i := 1; i < len(actives); i++ {
		if maxActive < actives[i] {
			maxActive = actives[i]
			c = ip.Circles[i]
		}
	}
	return
}

func (ip *Proxy) Query(w http.ResponseWriter, req *http.Request) (body []byte, err error) {
	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		return nil, ErrEmptyQuery
	}

	tokens, check, from := CheckQuery(q)
	if !check {
		return nil, ErrIllegalQL
	}

	checkDb, showDb, alterDb, db := CheckDatabaseFromTokens(tokens)
	if !checkDb {
		db = req.FormValue("db")
		if db == "" {
			db, _ = GetDatabaseFromTokens(tokens)
		}
	}
	if !showDb {
		if db == "" {
			return nil, ErrDatabaseNotFound
		}
		if len(ip.DBSet) > 0 && !ip.DBSet[db] {
			return nil, fmt.Errorf("database forbidden: %s", db)
		}
	}

	selectOrShow := CheckSelectOrShowFromTokens(tokens)
	if selectOrShow && from {
		// available circle -> backend by key(db,meas) -> select or show
		meas, err := GetMeasurementFromTokens(tokens)
		if err != nil {
			return nil, ErrGetMeasurement
		}
		key := GetKey(db, meas)
		badSet := make(map[int]bool)
		for {
			if len(badSet) == len(ip.Circles) {
				return nil, ErrBackendsUnavailable
			}
			id := rand.Intn(len(ip.Circles))
			if badSet[id] {
				continue
			}
			circle := ip.Circles[id]
			if circle.WriteOnly {
				badSet[id] = true
				continue
			}
			be := circle.GetBackend(key)
			if be.IsActive() {
				qr := be.Query(req, w, false)
				if qr.Status > 0 || len(badSet) == len(ip.Circles)-1 {
					return qr.Body, qr.Err
				}
			}
			badSet[id] = true
		}
	} else if selectOrShow && !from {
		// available circle -> all backends -> show
		badSet := make(map[int]bool)
		for {
			if len(badSet) == len(ip.Circles) {
				return ip.optimalCircle().Query(w, req, tokens)
			}
			id := rand.Intn(len(ip.Circles))
			if badSet[id] {
				continue
			}
			circle := ip.Circles[id]
			if circle.WriteOnly {
				badSet[id] = true
				continue
			}
			if circle.IsActive() {
				return circle.Query(w, req, tokens)
			}
			badSet[id] = true
		}
	} else if CheckDeleteOrDropMeasurementFromTokens(tokens) {
		// all circles -> backend by key(db,meas) -> delete or drop
		meas, err := GetMeasurementFromTokens(tokens)
		if err != nil {
			return nil, err
		}
		key := GetKey(db, meas)
		backends := ip.GetBackends(key)
		if len(backends) == 0 {
			return nil, ErrGetBackends
		}
		for _, be := range backends {
			if !be.IsActive() {
				return nil, fmt.Errorf("backend %s(%s) unavailable", be.Name, be.Url)
			}
		}
		bodies, _, err := QueryInParallel(backends, req, w, false)
		if err != nil {
			return nil, err
		}
		return bodies[0], nil
	} else if alterDb {
		// all circles -> all backends -> create or drop database
		for _, circle := range ip.Circles {
			if !circle.IsActive() {
				return nil, fmt.Errorf("circle %d unavailable", circle.CircleId)
			}
		}
		backends := make([]*Backend, 0)
		for _, circle := range ip.Circles {
			backends = append(backends, circle.Backends...)
		}
		bodies, _, err := QueryInParallel(backends, req, w, false)
		if err != nil {
			return nil, err
		}
		return bodies[0], nil
	}
	return nil, ErrIllegalQL
}

func (ip *Proxy) Write(p []byte, db, precision string) (err error) {
	buf := bytes.NewBuffer(p)
	var line []byte
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			log.Printf("error: %s", err)
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
		log.Printf("write data error: can't get backends")
		return
	}

	point := &LinePoint{db, nanoLine}
	for _, be := range backends {
		err := be.WritePoint(point)
		if err != nil {
			log.Printf("write data to buffer error: %s, %s, %s, %s, %s", err, be.Url, db, precision, string(line))
		}
	}
}
