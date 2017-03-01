package backend

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestScanKey(t *testing.T) {
	var points []string = []string{
		"cpu,host=server01,region=uswest value=1 1434055562000000000",
		"cpu value=3,value2=4 1434055562000010000",
		"temper\\ ature,machine=unit42,type=assembly internal=32,external=100 1434055562000000035",
		"temper\\,ature,machine=unit143,type=assembly internal=22,external=130 1434055562005000035",
	}
	var keys []string = []string{
		"cpu",
		"cpu",
		"temper ature",
		"temper,ature",
	}

	var key string
	var err error
	for i, s := range points {
		key, err = ScanKey([]byte(s))
		if err != nil {
			t.Errorf("error: %s", err)
			return
		}
		if key != keys[i] {
			t.Errorf("quota test failed: %s, %s", key, keys[i])
			return
		}
	}

	return
}

func BenchmarkScanKey(b *testing.B) {
	buf := &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(buf, "%s%d,a=%d,b=2 c=3 10000\n", "name", i, i)
	}
	b.ResetTimer()

	var err error
	var line []byte
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			b.Error(err)
			return
		case io.EOF, nil:
		}

		if len(line) == 0 {
			break
		}

		line = bytes.TrimRight(line, " \t\r\n")
		_, err = ScanKey(line)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func CreateTestInfluxCluster() (ic *InfluxCluster, err error) {
	redisConfig := &RedisConfigSource{}
	nodeConfig := &NodeConfig{}
	ic = NewInfluxCluster(redisConfig, nodeConfig)
	backends := make(map[string]BackendAPI)
	bkcfgs := make(map[string]*BackendConfig)
	cfg, _ := CreateTestBackendConfig("test1")
	bkcfgs["test1"] = cfg
	cfg, _ = CreateTestBackendConfig("test2")
	bkcfgs["test2"] = cfg
	for name, cfg := range bkcfgs {
		backends[name], err = NewBackends(cfg, name)
		if err != nil {
			return
		}
	}
	ic.backends = backends
	ic.nexts = "test2"
	ic.bas = append(ic.bas, backends["test2"])
	m2bs := make(map[string][]BackendAPI)
	m2bs["cpu"] = append(m2bs["cpu"], backends["test1"])
	ic.m2bs = m2bs

	return
}

func TestInfluxdbClusterWrite(t *testing.T) {
	ic, err := CreateTestInfluxCluster()
	if err != nil {
		t.Error(err)
		return
	}
	for i := 0; i < 100; i++ {
		err = ic.Write([]byte("cpu value=3,value2=4 1434055562000010000"))
		if err != nil {
			t.Error(err)
			return
		}
	}
	time.Sleep(time.Second)
}
func TestInfluxdbClusterPing(t *testing.T) {
	ic, err := CreateTestInfluxCluster()
	if err != nil {
		t.Error(err)
		return
	}
	version, err := ic.Ping()
	if err != nil {
		t.Error(err)
		return
	}
	if version == "" {
		t.Error("empty version")
		return
	}
	time.Sleep(time.Second)
}

func TestInfluxdbClusterQuery(t *testing.T) {
	ic, err := CreateTestInfluxCluster()
	if err != nil {
		t.Error(err)
		return
	}
	w := NewDummyResponseWriter()
	w.Header().Add("X-Influxdb-Version", VERSION)
	q := url.Values{}
	q.Set("db", "test")

	// test select * clause
	q.Set("q", "SELECT * from cpu where time > now() - 1m")
	req, _ := http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
	req.URL.Query()
	ic.Query(w, req)
	if w.status != 400 {
		t.Error("should be return 400 code")
		return
	}

	// test measurement not exist
	q.Set("q", "SELECT cpu_load from test WHERE time > now() - 1m")
	req, _ = http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
	req.URL.Query()
	ic.Query(w, req)
	if w.status != 400 {
		t.Error("should be return 400 code")
		return
	}

	// test where time clause
	q.Set("q", " select cpu_load from cpu WHERE time > now() - 1m")
	req, _ = http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
	req.URL.Query()
	ic.Query(w, req)
	if w.status != 200 && w.status != 204 {
		t.Error("should be return 200 or 204 code")
		return
	}
	// test show tag values from measurement
	q.Set("q", "SHOW tag keys from \"cpu\" ")
	req, _ = http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
	req.URL.Query()
	ic.Query(w, req)
	if w.status != 200 && w.status != 204 {
		t.Error("should be return 200 or 204 code")
		return
	}

	// test delete clause
	q.Set("q", " DELETE FROM \"cpu\" WHERE time < '2000-01-01T00:00:00Z'")
	req, _ = http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
	req.URL.Query()
	ic.Query(w, req)
	if w.status != 400 {
		t.Error("should be return 400 code")
		return
	}
}
