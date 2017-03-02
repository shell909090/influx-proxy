// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func HandlerAny(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	log.Printf("handler any get url: %s", req.URL)
	w.Header().Add("X-Influxdb-Version", VERSION)
	w.WriteHeader(204)
	return
}

func CreateTestBackendConfig(dbname string) (cfg *BackendConfig, ts *httptest.Server) {
	ts = httptest.NewServer(http.HandlerFunc(HandlerAny))
	cfg = &BackendConfig{
		URL:             ts.URL,
		DB:              dbname,
		Interval:        200,
		Timeout:         4000,
		TimeoutQuery:    6000,
		MaxRowLimit:     1000,
		CheckInterval:   1000,
		RewriteInterval: 1000,
	}
	return
}

func TestHttpBackendWrite(t *testing.T) {
	cfg, ts := CreateTestBackendConfig("test")
	defer ts.Close()
	hb := NewHttpBackend(cfg)
	defer hb.Close()

	err := hb.Write([]byte("cpu,host=server01,region=uswest value=1 1434055562000000000\ncpu value=3,value2=4 1434055562000010000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
}

func TestHttpBackendWriteCompressed(t *testing.T) {
	cfg, ts := CreateTestBackendConfig("test")
	defer ts.Close()
	hb := NewHttpBackend(cfg)
	defer hb.Close()

	var buf bytes.Buffer
	p := []byte("cpu,host=server01,region=uswest value=1 1434055562000000000\ncpu value=3,value2=4 1434055562000010000")
	err := Compress(&buf, p)
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
	p = buf.Bytes()
	err = hb.WriteCompressed(p)
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
}
func TestHttpBackendPing(t *testing.T) {
	cfg, ts := CreateTestBackendConfig("test")
	defer ts.Close()
	hb := NewHttpBackend(cfg)
	defer hb.Close()

	version, err := hb.Ping()
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
	if version == "" {
		t.Errorf("empty version")
	}
	return
}

type DummyResponseWriter struct {
	header http.Header
	status int
	buffer bytes.Buffer
}

func NewDummyResponseWriter() (drw *DummyResponseWriter) {
	drw = &DummyResponseWriter{
		header: make(http.Header, 1),
	}
	return
}

func (drw *DummyResponseWriter) Header() http.Header {
	return drw.header
}

func (drw *DummyResponseWriter) Write(p []byte) (n int, err error) {
	n, err = drw.buffer.Write(p)
	return
}

func (drw *DummyResponseWriter) WriteHeader(code int) {
	drw.status = code
	return
}

func TestHttpBackendQuery(t *testing.T) {
	cfg, ts := CreateTestBackendConfig("test")
	defer ts.Close()
	hb := NewHttpBackend(cfg)
	defer hb.Close()

	q := make(url.Values, 1)
	q.Set("db", "test")
	q.Set("q", "select * from cpu")

	req, err := http.NewRequest("GET", hb.URL+"/query?"+q.Encode(), nil)
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	w := NewDummyResponseWriter()

	err = hb.Query(w, req)
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	if w.status != 204 {
		t.Errorf("response error")
		return
	}
}
