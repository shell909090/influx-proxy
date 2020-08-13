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

	"github.com/chengshiwen/influx-proxy/util"
	mapset "github.com/deckarep/golang-set"
)

type Proxy struct {
	Circles    []*Circle
	dbList     []string
	dbSet      mapset.Set
	LogEnabled bool
}

func NewProxy(cfg *ProxyConfig) (ip *Proxy) {
	ip = &Proxy{
		Circles:    make([]*Circle, len(cfg.Circles)),
		dbList:     cfg.DbList,
		dbSet:      util.NewSetFromStrSlice(cfg.DbList),
		LogEnabled: cfg.LogEnabled,
	}
	for idx, circfg := range cfg.Circles {
		ip.Circles[idx] = NewCircle(circfg, cfg, idx)
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
			if circle.WriteOnly {
				badIds.Add(id)
				continue
			}
			if circle.CheckActive() {
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
			be := circle.GetBackend(key)
			ip.Logf("query circle: %d backend: %s", circle.CircleId, be.Url)
			return be.Query(req, w, false)
		}
		// available circle -> all backends -> show
		ip.Logf("query circle: %d", circle.CircleId)
		return circle.Query(w, req, tokens)
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
		for _, be := range backends {
			ip.Logf("query backend: %s", be.Url)
			req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
			body, err = be.Query(req, w, false)
			if err != nil {
				return nil, err
			}
		}
	} else if alterDb {
		// all circles -> all backends -> create or drop database
		for _, circle := range ip.Circles {
			if !circle.CheckActive() {
				return nil, fmt.Errorf("circle %d not health", circle.CircleId)
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
	for _, be := range backends {
		err := be.WritePoint(point)
		if err != nil {
			log.Printf("write data to buffer error: %s, %s, %s, %s, %s", err, be.Url, db, precision, string(line))
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
