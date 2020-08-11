package backend

import (
	"bytes"
	"fmt"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/deckarep/golang-set"
	"github.com/influxdata/influxdb1-client/models"
	"io/ioutil"
	"net/http"
	"stathat.com/c/consistent"
	"strconv"
	"strings"
	"sync"
	"time"
)

var FieldDataTypes = []string{"float", "integer", "string", "boolean"}

type Circle struct {
	CircleId     int
	Name         string
	Backends     []*Backend
	router       *consistent.Consistent
	routerCaches map[string]*Backend
	mapToBackend map[string]*Backend
	BackendWgMap map[string]*sync.WaitGroup
	IsMigrating  bool
	MigrateWg    *sync.WaitGroup
}

func NewCircle(cfg *CircleConfig, pxcfg *ProxyConfig, circleId int) (ic *Circle) {
	ic = &Circle{
		CircleId:     circleId,
		Name:         cfg.Name,
		Backends:     make([]*Backend, len(cfg.Backends)),
		router:       consistent.New(),
		routerCaches: make(map[string]*Backend),
		mapToBackend: make(map[string]*Backend),
		BackendWgMap: make(map[string]*sync.WaitGroup),
		IsMigrating:  false,
		MigrateWg:    &sync.WaitGroup{},
	}
	ic.router.NumberOfReplicas = pxcfg.VNodeSize
	for idx, bkcfg := range cfg.Backends {
		backend := NewBackend(bkcfg, pxcfg)
		ic.Backends[idx] = backend
		ic.BackendWgMap[bkcfg.Url] = &sync.WaitGroup{}
		ic.addRouter(backend, idx, pxcfg.HashKey)
	}
	return
}

func (ic *Circle) addRouter(ib *Backend, idx int, hashKey string) {
	if hashKey == "name" {
		ic.router.Add(ib.Name)
		ic.mapToBackend[ib.Name] = ib
	} else if hashKey == "url" {
		ic.router.Add(ib.Url)
		ic.mapToBackend[ib.Url] = ib
	} else {
		ic.router.Add(strconv.Itoa(idx))
		ic.mapToBackend[strconv.Itoa(idx)] = ib
	}
}

func (ic *Circle) GetBackend(key string) *Backend {
	if backend, ok := ic.routerCaches[key]; ok {
		return backend
	} else {
		value, _ := ic.router.Get(key)
		backend := ic.mapToBackend[value]
		ic.routerCaches[key] = backend
		return backend
	}
}

func (ic *Circle) GetHealth() []map[string]interface{} {
	stats := make([]map[string]interface{}, len(ic.Backends))
	for i, b := range ic.Backends {
		stats[i] = b.GetHealth(ic)
	}
	return stats
}

func (ic *Circle) CheckStatus() bool {
	for _, backend := range ic.Backends {
		if !backend.Active {
			return false
		}
	}
	return true
}

func (ic *Circle) Query(w http.ResponseWriter, req *http.Request, tokens []string) ([]byte, error) {
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	var reqBodyBytes []byte
	if req.Body != nil {
		reqBodyBytes, _ = ioutil.ReadAll(req.Body)
	}
	bodies := make([][]byte, 0)

	for _, backend := range ic.Backends {
		req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
		body, err := backend.Query(req, w, true)
		if err != nil {
			return nil, err
		}
		if body != nil {
			bodies = append(bodies, body)
		}
	}

	var rsp *Response
	var err error
	stmt2 := GetHeadStmtFromTokens(tokens, 2)
	stmt3 := GetHeadStmtFromTokens(tokens, 3)
	if stmt2 == "show measurements" || stmt2 == "show series" || stmt2 == "show databases" {
		rsp, err = ic.reduceByValues(bodies)
	} else if stmt3 == "show field keys" || stmt3 == "show tag keys" || stmt3 == "show tag values" {
		rsp, err = ic.reduceBySeries(bodies)
	} else if stmt3 == "show retention policies" {
		rsp, err = ic.concatByValues(bodies)
	} else if stmt2 == "show stats" {
		rsp, err = ic.concatByResults(bodies)
	}
	if err != nil {
		return nil, err
	}
	if rsp == nil {
		rsp = ResponseFromSeries(nil)
	}
	pretty := req.FormValue("pretty") == "true"
	body := util.MarshalJson(rsp, pretty, true)
	if w.Header().Get("Content-Encoding") == "gzip" {
		return util.GzipCompress(body)
	} else {
		return body, nil
	}
}

func (ic *Circle) reduceByValues(bodies [][]byte) (rsp *Response, err error) {
	valuesMap := make(map[string]*models.Row)
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		for _, s := range _series {
			for _, value := range s.Values {
				if len(value) < 1 {
					continue
				}
				key := value[0].(string)
				valuesMap[key] = s
			}
		}
	}
	serie := &models.Row{}
	var values [][]interface{}
	for v, s := range valuesMap {
		values = append(values, []interface{}{v})
		serie = s
	}
	if len(values) > 0 {
		serie.Values = values
		rsp = ResponseFromSeries(models.Rows{serie})
	} else {
		rsp = ResponseFromSeries(nil)
	}
	return
}

func (ic *Circle) reduceBySeries(bodies [][]byte) (rsp *Response, err error) {
	seriesMap := make(map[string]*models.Row)
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		for _, s := range _series {
			seriesMap[s.Name] = s
		}
	}

	var series models.Rows
	for _, item := range seriesMap {
		series = append(series, item)
	}
	return ResponseFromSeries(series), nil
}

func (ic *Circle) concatByValues(bodies [][]byte) (rsp *Response, err error) {
	var series []*models.Row
	var values [][]interface{}
	for _, b := range bodies {
		_series, err := SeriesFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		if len(_series) == 1 {
			series = _series
			for _, value := range _series[0].Values {
				values = append(values, value)
			}
		}
	}
	if len(series) == 1 {
		series[0].Values = values
	}
	return ResponseFromSeries(series), nil
}

func (ic *Circle) concatByResults(bodies [][]byte) (rsp *Response, err error) {
	var results []*Result
	for _, b := range bodies {
		_results, err := ResultsFromResponseBytes(b)
		if err != nil {
			return nil, err
		}
		if len(_results) == 1 {
			results = append(results, _results[0])
		}
	}
	return ResponseFromResults(results), nil
}

func (ic *Circle) Migrate(srcBackend *Backend, dstBackends []*Backend, db, meas string, seconds int, batch int) error {
	timeClause := ""
	if seconds > 0 {
		timeClause = fmt.Sprintf(" where time >= %ds", time.Now().Unix()-int64(seconds))
	}

	rsp, err := srcBackend.QueryIQL(db, fmt.Sprintf("select * from \"%s\"%s", meas, timeClause))
	if err != nil {
		return err
	}
	series, err := SeriesFromResponseBytes(rsp)
	if err != nil {
		return err
	}
	if len(series) < 1 {
		return nil
	}
	columns := series[0].Columns

	tagKeys := srcBackend.GetTagKeys(db, meas)
	tagMap := util.NewSetFromStrSlice(tagKeys)
	fieldKeys := srcBackend.GetFieldKeys(db, meas)
	fieldMap := reformFieldKeys(fieldKeys)

	valen := len(series[0].Values)
	lines := make([]string, 0, util.MinInt(valen, batch))
	for idx, value := range series[0].Values {
		mtagSet := []string{util.EscapeMeasurement(meas)}
		fieldSet := make([]string, 0)
		for i := 1; i < len(value); i++ {
			k := columns[i]
			v := value[i]
			if tagMap.Contains(k) {
				if v != nil {
					mtagSet = append(mtagSet, fmt.Sprintf("%s=%s", util.EscapeTag(k), util.EscapeTag(v.(string))))
				}
			} else if vtype, ok := fieldMap[k]; ok {
				if v != nil {
					if vtype == "float" || vtype == "boolean" {
						fieldSet = append(fieldSet, fmt.Sprintf("%s=%v", util.EscapeTag(k), v))
					} else if vtype == "integer" {
						fieldSet = append(fieldSet, fmt.Sprintf("%s=%vi", util.EscapeTag(k), v))
					} else if vtype == "string" {
						fieldSet = append(fieldSet, fmt.Sprintf("%s=\"%s\"", util.EscapeTag(k), models.EscapeStringField(v.(string))))
					}
				}
			}
		}
		mtagStr := strings.Join(mtagSet, ",")
		fieldStr := strings.Join(fieldSet, ",")
		ts, _ := time.Parse(time.RFC3339Nano, value[0].(string))
		line := fmt.Sprintf("%s %s %d", mtagStr, fieldStr, ts.UnixNano())
		lines = append(lines, line)
		if (idx+1)%batch == 0 || idx+1 == valen {
			if len(lines) != 0 {
				lineData := strings.Join(lines, "\n")
				for _, dstBackend := range dstBackends {
					err = dstBackend.Write(db, []byte(lineData))
					if err != nil {
						return err
					}
				}
				lines = lines[:0]
			}
		}
	}
	return nil
}

func reformFieldKeys(fieldKeys map[string][]string) map[string]string {
	// The SELECT statement returns all field values if all values have the same type.
	// If field value types differ across shards, InfluxDB first performs any applicable cast operations and
	// then returns all values with the type that occurs first in the following list: float, integer, string, boolean.
	fieldSet := make(map[string]mapset.Set, len(fieldKeys))
	for field, types := range fieldKeys {
		fieldSet[field] = util.NewSetFromStrSlice(types)
	}
	fieldMap := make(map[string]string, len(fieldKeys))
	for field, types := range fieldKeys {
		if len(types) == 1 {
			fieldMap[field] = types[0]
		} else {
			for _, dt := range FieldDataTypes {
				if fieldSet[field].Contains(dt) {
					fieldMap[field] = dt
					break
				}
			}
		}
	}
	return fieldMap
}
