package transfer

import (
	"fmt"
	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/util"
	"github.com/deckarep/golang-set"
	"github.com/influxdata/influxdb1-client/models"
	"github.com/panjf2000/ants/v2"
	"gopkg.in/natefinch/lumberjack.v2"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	FieldTypes = []string{"float", "integer", "string", "boolean"}
	tlog       = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
)

type Transfer struct {
	username     string
	password     string
	authSecure   bool
	httpsEnabled bool

	pool         *ants.Pool
	tlogDir      string
	CircleStates []*CircleState
	Worker       int
	Batch        int
	Resyncing    bool
	HaAddrs      []string
	lock         sync.RWMutex
}

func NewTransfer(cfg *backend.ProxyConfig, circles []*backend.Circle) (tx *Transfer) {
	tx = &Transfer{
		tlogDir:      cfg.MlogDir,
		CircleStates: make([]*CircleState, len(cfg.Circles)),
		Worker:       1,
		Batch:        25000,
	}
	for idx, circfg := range cfg.Circles {
		tx.CircleStates[idx] = NewCircleState(circfg, circles[idx])
	}
	return
}

func (tx *Transfer) resetCircleStates() {
	for _, cs := range tx.CircleStates {
		cs.ResetStates()
	}
}

func (tx *Transfer) setLogOutput(name string) {
	logPath := filepath.Join(tx.tlogDir, name)
	if logPath == "" {
		tlog.SetOutput(os.Stdout)
	} else {
		util.MakeDir(tx.tlogDir)
		tlog.SetOutput(&lumberjack.Logger{
			Filename:   logPath,
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     7,
		})
	}
}

func (tx *Transfer) getDatabases() []string {
	for _, c := range tx.CircleStates {
		for _, b := range c.Backends {
			if b.Active {
				return b.GetDatabases()
			}
		}
	}
	return nil
}

func getBackendUrls(backends []*backend.Backend) []string {
	backendUrls := make([]string, len(backends))
	for k, b := range backends {
		backendUrls[k] = b.Url
	}
	return backendUrls
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
			for _, dt := range FieldTypes {
				if fieldSet[field].Contains(dt) {
					fieldMap[field] = dt
					break
				}
			}
		}
	}
	return fieldMap
}

func (tx *Transfer) transfer(src *backend.Backend, dsts []*backend.Backend, db, meas string, secs int) error {
	timeClause := ""
	if secs > 0 {
		timeClause = fmt.Sprintf(" where time >= %ds", time.Now().Unix()-int64(secs))
	}

	rsp, err := src.QueryIQL(db, fmt.Sprintf("select * from \"%s\"%s", meas, timeClause))
	if err != nil {
		return err
	}
	series, err := backend.SeriesFromResponseBytes(rsp)
	if err != nil {
		return err
	}
	if len(series) < 1 {
		return nil
	}
	columns := series[0].Columns

	tagKeys := src.GetTagKeys(db, meas)
	tagMap := util.NewSetFromStrSlice(tagKeys)
	fieldKeys := src.GetFieldKeys(db, meas)
	fieldMap := reformFieldKeys(fieldKeys)

	valen := len(series[0].Values)
	lines := make([]string, 0, util.MinInt(valen, tx.Batch))
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
		if (idx+1)%tx.Batch == 0 || idx+1 == valen {
			if len(lines) != 0 {
				lineData := strings.Join(lines, "\n")
				for _, dst := range dsts {
					err = dst.Write(db, []byte(lineData))
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

func (tx *Transfer) submitTransfer(cs *CircleState, src *backend.Backend, dsts []*backend.Backend, db, meas string, secs int) {
	cs.wg.Add(1)
	tx.pool.Submit(func() {
		defer cs.wg.Done()
		err := tx.transfer(src, dsts, db, meas, secs)
		if err == nil {
			tlog.Printf("transfer done, circle:%d src:%s dst:%v db:%s meas:%s secs:%d", cs.CircleId, src.Url, getBackendUrls(dsts), db, meas, secs)
		} else {
			tlog.Printf("transfer error: %s, circle:%d src:%s dst:%v db:%s meas:%s secs:%d", err, cs.CircleId, src.Url, getBackendUrls(dsts), db, meas, secs)
		}
	})
}

func (tx *Transfer) submitCleanup(cs *CircleState, be *backend.Backend, db, meas string) {
	cs.wg.Add(1)
	tx.pool.Submit(func() {
		defer cs.wg.Done()
		_, err := be.DropMeasurement(db, meas)
		if err == nil {
			tlog.Printf("cleanup done, circle:%d backend:%s db:%s meas:%s", cs.CircleId, be.Url, db, meas)
		} else {
			tlog.Printf("cleanup error: %s, circle:%d backend:%s db:%s meas:%s", err, cs.CircleId, be.Url, db, meas)
		}
	})
}

func (tx *Transfer) runTransfer(cs *CircleState, be *backend.Backend, dbs []string, f func(*CircleState, *backend.Backend, string, string, ...interface{}) bool, args ...interface{}) {
	if !be.Active {
		tlog.Printf("backend not active: %s", be.Url)
		return
	}

	stats := cs.Stats[be.Url]
	stats.DatabaseTotal = int32(len(dbs))
	measures := make(map[string][]string)
	for _, db := range dbs {
		measures[db] = be.GetMeasurements(db)
		stats.MeasurementTotal += int32(len(measures[db]))
	}

	for _, db := range dbs {
		for _, meas := range measures[db] {
			// db := db
			// meas := meas
			require := f(cs, be, db, meas, args)
			if require {
				atomic.AddInt32(&stats.TransferCount, 1)
			} else {
				atomic.AddInt32(&stats.InPlaceCount, 1)
			}
			atomic.AddInt32(&stats.MeasurementDone, 1)
		}
		atomic.AddInt32(&stats.DatabaseDone, 1)
	}
}

func (tx *Transfer) Rebalance(circleId int, backends []*backend.Backend, dbs []string) {
	tx.setLogOutput("rebalance.log")
	tlog.Printf("rebalance start")
	cs := tx.CircleStates[circleId]
	tx.resetCircleStates()
	tx.SetTransferringAndBroadcast(cs, true)
	defer tx.SetTransferringAndBroadcast(cs, false)
	if len(dbs) == 0 {
		dbs = tx.getDatabases()
	}

	tx.pool, _ = ants.NewPool(tx.Worker)
	defer tx.pool.Release()
	for _, be := range backends {
		go tx.runTransfer(cs, be, dbs, tx.runRebalance)
	}
	cs.wg.Wait()
	tlog.Printf("rebalance done")
}

func (tx *Transfer) runRebalance(cs *CircleState, be *backend.Backend, db string, meas string, args ...interface{}) (require bool) {
	key := backend.GetKey(db, meas)
	dst := cs.GetBackend(key)
	require = dst.Url != be.Url
	if require {
		tx.submitTransfer(cs, be, []*backend.Backend{dst}, db, meas, 0)
	}
	return
}

func (tx *Transfer) Recovery(fromCircleId, toCircleId int, recoveryUrls []string, dbs []string) {
	tx.setLogOutput("recovery.log")
	tlog.Printf("recovery start")
	fcs := tx.CircleStates[fromCircleId]
	tcs := tx.CircleStates[toCircleId]
	tx.resetCircleStates()
	tx.SetTransferringAndBroadcast(tcs, true)
	defer tx.SetTransferringAndBroadcast(tcs, false)
	if len(dbs) == 0 {
		dbs = tx.getDatabases()
	}

	tx.pool, _ = ants.NewPool(tx.Worker)
	defer tx.pool.Release()
	recoveryUrlSet := mapset.NewSet()
	if len(recoveryUrls) != 0 {
		for _, u := range recoveryUrls {
			recoveryUrlSet.Add(u)
		}
	} else {
		for _, b := range tcs.Backends {
			recoveryUrlSet.Add(b.Url)
		}
	}
	for _, be := range fcs.Backends {
		go tx.runTransfer(fcs, be, dbs, tx.runRecovery, tcs, recoveryUrlSet)
	}
	fcs.wg.Wait()
	tlog.Printf("recovery done")
}

func (tx *Transfer) runRecovery(fcs *CircleState, be *backend.Backend, db string, meas string, args ...interface{}) (require bool) {
	tcs := args[0].(*CircleState)
	recoveryUrlSet := args[1].(mapset.Set)
	key := backend.GetKey(db, meas)
	dst := tcs.GetBackend(key)
	require = recoveryUrlSet.Contains(dst.Url)
	if require {
		tx.submitTransfer(fcs, be, []*backend.Backend{dst}, db, meas, 0)
	}
	return
}

func (tx *Transfer) Resync(dbs []string, secs int) {
	tx.setLogOutput("resync.log")
	tlog.Printf("resync start")
	tx.resetCircleStates()
	tx.SetResyncingAndBroadcast(true)
	defer tx.SetResyncingAndBroadcast(false)
	if len(dbs) == 0 {
		dbs = tx.getDatabases()
	}

	tx.pool, _ = ants.NewPool(tx.Worker)
	defer tx.pool.Release()
	for _, cs := range tx.CircleStates {
		for _, be := range cs.Backends {
			go tx.runTransfer(cs, be, dbs, tx.runResync, secs)
		}
		cs.wg.Wait()
		tlog.Printf("circle %d resync done", cs.CircleId)
	}
	tlog.Printf("resync done")
}

func (tx *Transfer) runResync(cs *CircleState, be *backend.Backend, db string, meas string, args ...interface{}) (require bool) {
	secs := args[0].(int)
	key := backend.GetKey(db, meas)
	dsts := make([]*backend.Backend, 0)
	for _, tcs := range tx.CircleStates {
		if tcs.CircleId != cs.CircleId {
			dst := tcs.GetBackend(key)
			dsts = append(dsts, dst)
		}
	}
	require = len(dsts) > 0
	if require {
		tx.submitTransfer(cs, be, dsts, db, meas, secs)
	}
	return
}

func (tx *Transfer) Cleanup(circleId int) {
	tx.setLogOutput("cleanup.log")
	tlog.Printf("cleanup start")
	cs := tx.CircleStates[circleId]
	tx.resetCircleStates()
	tx.SetTransferringAndBroadcast(cs, true)
	defer tx.SetTransferringAndBroadcast(cs, false)

	tx.pool, _ = ants.NewPool(tx.Worker)
	defer tx.pool.Release()
	for _, be := range cs.Backends {
		dbs := be.GetDatabases()
		go tx.runTransfer(cs, be, dbs, tx.runCleanup)
	}
	cs.wg.Wait()
	tlog.Printf("cleanup done")
}

func (tx *Transfer) runCleanup(cs *CircleState, be *backend.Backend, db string, meas string, args ...interface{}) (require bool) {
	tlog.Printf("check circle:%d backend:%s db:%s meas:%s", cs.CircleId, be.Url, db, meas)
	key := backend.GetKey(db, meas)
	dst := cs.GetBackend(key)
	require = dst.Url != be.Url
	if require {
		tlog.Printf("cleanup circle:%d backend:%s db:%s meas:%s should transfer to %s", cs.CircleId, be.Url, db, meas, dst.Url)
		tx.submitCleanup(cs, be, db, meas)
	}
	return
}

func (tx *Transfer) SetResyncing(resyncing bool) {
	tx.lock.Lock()
	defer tx.lock.Unlock()
	tx.Resyncing = resyncing
}

func (tx *Transfer) SetTransferring(cs *CircleState, transferring bool) {
	tx.lock.Lock()
	defer tx.lock.Unlock()
	cs.Transferring = transferring
}

func (tx *Transfer) SetResyncingAndBroadcast(resyncing bool) {
	tx.SetResyncing(resyncing)
	client := backend.NewClient(tx.httpsEnabled, 10)
	for _, addr := range tx.HaAddrs {
		url := fmt.Sprintf("http://%s/transfer/state?resyncing=%t", addr, resyncing)
		tx.PostBroadcast(client, url)
	}
}

func (tx *Transfer) SetTransferringAndBroadcast(cs *CircleState, transferring bool) {
	tx.SetTransferring(cs, transferring)
	client := backend.NewClient(tx.httpsEnabled, 10)
	for _, addr := range tx.HaAddrs {
		url := fmt.Sprintf("http://%s/transfer/state?circle_id=%d&transferring=%t", addr, cs.CircleId, transferring)
		tx.PostBroadcast(client, url)
	}
}

func (tx *Transfer) PostBroadcast(client *http.Client, url string) {
	if tx.httpsEnabled {
		url = strings.Replace(url, "http", "https", 1)
	}
	req, _ := http.NewRequest("POST", url, nil)
	if tx.username != "" || tx.password != "" {
		backend.SetBasicAuth(req, tx.username, tx.password, tx.authSecure)
	}
	client.Do(req)
}
