// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

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
	tests := []struct {
		name string
		args []byte
		want error
	}{
		{
			name: "cpu",
			args: []byte("cpu value=3,value2=4 1434055562000010000"),
			want: nil,
		},
		{
			name: "cpu.load",
			args: []byte("cpu.load value=3,value2=4 1434055562000010000"),
			want: nil,
		},
		{
			name: "load.cpu",
			args: []byte("load.cpu value=3,value2=4 1434055562000010000"),
			want: nil,
		},
		{
			name: "test",
			args: []byte("test value=3,value2=4 1434055562000010000"),
		},
	}
	for _, tt := range tests {
		err := ic.Write(tt.args)
		if err != nil {
			t.Error(tt.name, err)
			continue
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

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{
			name:  "cpu",
			query: "SELECT * from cpu where time > now() - 1m",
			want:  400,
		},
		{
			name:  "test",
			query: "SELECT cpu_load from test WHERE time > now() - 1m",
			want:  400,
		},
		{
			name:  "cpu_load",
			query: " select cpu_load from cpu WHERE time > now() - 1m ",
			want:  400,
		},
		{
			name:  "cpu_load",
			query: " select cpu_load from cpu WHERE time > now() - 1m group by time(10s)",
			want:  204,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m GROUP by time(10s)",
			want:  204,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m GROUP    BY  time(10s)",
			want:  204,
		},
		{
			name:  "load.cpu",
			query: " select cpu_load from \"load.cpu\" WHERE time > now() - 1m",
			want:  400,
		},
		{
			name:  "show_cpu",
			query: "SHOW tag keys from \"cpu\" ",
			want:  204,
		},
		{
			name:  "delete_cpu",
			query: " DELETE FROM \"cpu\" WHERE time < '2000-01-01T00:00:00Z'",
			want:  400,
		},
		{
			name:  "show_measurements",
			query: "SHOW measurements ",
			want:  200,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m and host =~ /()$/",
			want:  400,
		},
		{
			name:  "cpu.load",
			query: " select cpu_load from \"cpu.load\" WHERE time > now() - 1m and host =~ /^()$/",
			want:  400,
		},
	}

	for _, tt := range tests {
		q.Set("q", tt.query)
		req, _ := http.NewRequest("GET", "http://localhost:8086/query?"+q.Encode(), nil)
		req.URL.Query()
		ic.Query(w, req)
		if w.status != tt.want {
			t.Error(tt.name, err, w.status)
		}
	}
}
