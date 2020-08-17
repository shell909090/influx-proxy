package backend

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var (
	ErrEmptyQuery       = errors.New("empty query")
	ErrDatabaseNotFound = errors.New("database not found")
	ErrEmptyMeasurement = errors.New("can't get measurement")
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
				return nil, errors.New("circles unavailable")
			}
			time.Sleep(time.Microsecond)
		}
		if from {
			meas, err := GetMeasurementFromTokens(tokens)
			if err != nil {
				return nil, ErrEmptyMeasurement
			}
			// available circle -> key(db,meas) -> backend -> select or show
			key := GetKey(db, meas)
			be := circle.GetBackend(key)
			// log.Printf("query circle: %d backend: %s", circle.CircleId, be.Url)
			return be.Query(req, w, false)
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
		var bodyBytes []byte
		if req.Body != nil {
			bodyBytes, _ = ioutil.ReadAll(req.Body)
		}
		key := GetKey(db, meas)
		backends := ip.GetBackends(key)
		for _, be := range backends {
			// log.Printf("query backend: %s", be.Url)
			req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
			body, err = be.Query(req, w, false)
			if err != nil {
				return nil, err
			}
		}
		return body, nil
	} else if alterDb {
		// all circles -> all backends -> create or drop database
		for _, circle := range ip.Circles {
			if !circle.CheckActive() {
				return nil, fmt.Errorf("circle %d not health", circle.CircleId)
			}
		}
		for _, circle := range ip.Circles {
			// log.Printf("query circle: %d", circle.CircleId)
			body, err = circle.Query(w, req, tokens)
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
		log.Printf("write data error: get backends return 0")
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
