package backend

import (
	"bytes"
	"fmt"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/influxdata/influxdb1-client/models"
	"io/ioutil"
	"log"
	"net/http"
	"stathat.com/c/consistent"
	"strings"
	"sync"
	"time"
)

type Circle struct {
	Name         string                     `json:"name"`
	CircleId     int                        `json:"circle_id"`
	Router       *consistent.Consistent     `json:"router"`
	Backends     []*Backend                 `json:"backends"`
	MapToBackend map[string]*Backend        `json:"map_to_backend"`
	BackendWgMap map[string]*sync.WaitGroup `json:"backend_wg_map"`
	IsMigrating  bool                       `json:"is_migrating"`
	MigrateWg    *sync.WaitGroup            `json:"migrate_wg"`
}

func (circle *Circle) GetBackend(key string) *Backend {
	value, _ := circle.Router.Get(key)
	return circle.MapToBackend[value]
}

func (circle *Circle) GetHealth() []map[string]interface{} {
	stats := make([]map[string]interface{}, len(circle.Backends))
	for i, b := range circle.Backends {
		stats[i] = map[string]interface{}{
			"name":    b.Name,
			"url":     b.Url,
			"active":  b.Active,
			"backlog": b.IsData(),
			"rewrite": b.RewriteRunning,
			"load":    circle.GetBackendLoad(b),
		}
	}
	return stats
}

func (circle *Circle) GetBackendLoad(b *Backend) map[string]map[string]int {
	load := make(map[string]map[string]int)
	dbs := b.GetDatabases()
	for _, db := range dbs {
		inplace, incorrect := 0, 0
		measurements := b.GetMeasurements(db)
		for _, meas := range measurements {
			key := GetKey(db, meas)
			nb := circle.GetBackend(key)
			if nb.Url == b.Url {
				inplace++
			} else {
				incorrect++
			}
		}
		load[db] = map[string]int{"measurements": len(measurements), "inplace": inplace, "incorrect": incorrect}
	}
	return load
}

func (circle *Circle) CheckStatus() bool {
	for _, backend := range circle.Backends {
		if !backend.Active {
			return false
		}
	}
	return true
}

func (circle *Circle) Query(w http.ResponseWriter, req *http.Request, tokens []string) ([]byte, error) {
	// remove support of query parameter `chunked`
	req.Form.Del("chunked")
	var reqBodyBytes []byte
	if req.Body != nil {
		reqBodyBytes, _ = ioutil.ReadAll(req.Body)
	}
	bodies := make([][]byte, 0)

	for _, backend := range circle.Backends {
		req.Body = ioutil.NopCloser(bytes.NewBuffer(reqBodyBytes))
		body, err := backend.Query(req, w, true)
		if err != nil {
			log.Printf("backend query error: %s %s", backend.Url, err)
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
		rsp, err = circle.reduceByValues(bodies)
	} else if stmt3 == "show field keys" || stmt3 == "show tag keys" || stmt3 == "show tag values" {
		rsp, err = circle.reduceBySeries(bodies)
	} else if stmt3 == "show retention policies" {
		rsp, err = circle.concatByValues(bodies)
	} else if stmt2 == "show stats" {
		rsp, err = circle.concatByResults(bodies)
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

func (circle *Circle) reduceByValues(bodies [][]byte) (rsp *Response, err error) {
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

func (circle *Circle) reduceBySeries(bodies [][]byte) (rsp *Response, err error) {
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

func (circle *Circle) concatByValues(bodies [][]byte) (rsp *Response, err error) {
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

func (circle *Circle) concatByResults(bodies [][]byte) (rsp *Response, err error) {
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

func (circle *Circle) Migrate(srcBackend *Backend, dstBackends []*Backend, db, meas string, seconds int, batch int) error {
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
	tagMap := make(map[string]bool, 0)
	for _, t := range tagKeys {
		tagMap[t] = true
	}
	fieldKeys := srcBackend.GetFieldKeys(db, meas)

	vlen := len(series[0].Values)
	var lines []string
	for idx, value := range series[0].Values {
		mtagSet := []string{util.EscapeMeasurement(meas)}
		fieldSet := make([]string, 0)
		for i := 1; i < len(value); i++ {
			k := columns[i]
			v := value[i]
			if _, ok := tagMap[k]; ok {
				if v != nil {
					mtagSet = append(mtagSet, fmt.Sprintf("%s=%s", util.EscapeTag(k), util.EscapeTag(v.(string))))
				}
			} else if vtype, ok := fieldKeys[k]; ok {
				if v != nil {
					if vtype == "float" || vtype == "boolean" {
						fieldSet = append(fieldSet, fmt.Sprintf("%s=%v", util.EscapeTag(k), v))
					} else if vtype == "integer" {
						fieldSet = append(fieldSet, fmt.Sprintf("%s=%di", util.EscapeTag(k), int64(v.(float64))))
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
		if (idx+1)%batch == 0 || idx+1 == vlen {
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
