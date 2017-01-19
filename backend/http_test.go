package backend

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
)

var (
	TESTCFG = &BackendConfig{
		URL:          "http://localhost:8086",
		DB:           "test",
		Interval:     100,
		Timeout:      10,
		TimeoutQuery: 60,
	}
)

func TestHttpBackendWrite(t *testing.T) {
	hb := NewHttpBackend(TESTCFG)

	err := hb.Write([]byte("cpu,host=server01,region=uswest value=1 1434055562000000000\ncpu value=3,value2=4 1434055562000010000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
}

func TestHttpBackendPing(t *testing.T) {
	hb := NewHttpBackend(TESTCFG)

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
	hb := NewHttpBackend(TESTCFG)

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

	buf := w.buffer.Bytes()
	if len(buf) == 0 {
		t.Errorf("response empty")
		return
	}

	VerifyResults(t, buf)
}

type Results map[string][]map[string]interface{}

func VerifyResults(t *testing.T, buf []byte) {
	var r Results
	err := json.Unmarshal(buf, &r)
	if err != nil {
		t.Errorf("json decode error")
		return
	}

	rr, ok := r["results"]
	if !ok {
		t.Errorf("no results")
		return
	}

	if len(rr) != 1 {
		t.Errorf("no results")
		return
	}
	rrr := rr[0]

	name, ok := rrr["name"]
	if !ok {
		t.Errorf("no name")
		return
	}
	if name != "cpu" {
		t.Errorf("wrong name")
		return
	}

	return
}
