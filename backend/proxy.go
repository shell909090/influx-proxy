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
)

var (
	ErrEmptyQuery         = errors.New("empty query")
	ErrDatabaseNotFound   = errors.New("database not found")
	ErrUnavailableCircles = errors.New("circles unavailable")
	ErrGetMeasurement     = errors.New("can't get measurement")
	ErrGetBackends        = errors.New("can't get backends")
)

type Proxy struct {
	Circles []*Circle
	DBSet   map[string]bool
}

func NewProxy(cfg *ProxyConfig) (ip *Proxy) {
	ip = &Proxy{
		Circles: make([]*Circle, len(cfg.Circles)),
		DBSet:   make(map[string]bool),
	}
	for idx, circfg := range cfg.Circles {
		ip.Circles[idx] = NewCircle(circfg, cfg, idx)
	}
	for _, db := range cfg.DBList {
		ip.DBSet[db] = true
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
		backends[i] = circle.GetBackend(key)
	}
	return backends
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

	if CheckSelectOrShowFromTokens(tokens) {
		var circle *Circle
		badSet := make(map[int]bool)
		for {
			id := rand.Intn(len(ip.Circles))
			if badSet[id] {
				continue
			}
			circle = ip.Circles[id]
			if circle.WriteOnly {
				badSet[id] = true
				continue
			}
			if circle.CheckActive() {
				break
			}
			badSet[id] = true
			if len(badSet) == len(ip.Circles) {
				return nil, ErrUnavailableCircles
			}
			time.Sleep(time.Microsecond)
		}
		if from {
			meas, err := GetMeasurementFromTokens(tokens)
			if err != nil {
				return nil, ErrGetMeasurement
			}
			// available circle -> key(db,meas) -> backend -> select or show
			key := GetKey(db, meas)
			be := circle.GetBackend(key)
			// log.Printf("query circle: %d backend: %s", circle.CircleId, be.Url)
			qr := be.Query(req, false)
			CopyHeader(w.Header(), qr.Header)
			return qr.Body, qr.Err
		}
		// available circle -> all backends -> show
		// log.Printf("query circle: %d", circle.CircleId)
		return circle.Query(w, req, tokens)
	} else if CheckDeleteOrDropMeasurementFromTokens(tokens) {
		// all circles -> key(db,meas) -> backend -> delete or drop
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
			if !be.Active {
				return nil, fmt.Errorf("backend %s(%s) not active", be.Name, be.Url)
			}
		}
		bodies, _, err := ParallelQuery(backends, req, w, false)
		if err != nil {
			return nil, err
		}
		return bodies[0], nil
	} else if alterDb {
		// all circles -> all backends -> create or drop database
		for _, circle := range ip.Circles {
			if !circle.CheckActive() {
				return nil, fmt.Errorf("circle %d not active", circle.CircleId)
			}
		}
		var wg = sync.WaitGroup{}
		for _, circle := range ip.Circles {
			// log.Printf("query circle: %d", circle.CircleId)
			wg.Add(1)
			go func(circle *Circle) {
				defer wg.Done()
				body, err = circle.Query(w, req, tokens)
			}(circle)
			wg.Wait()
			if err != nil {
				return
			}
		}
		return body, nil
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
