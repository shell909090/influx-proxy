package backend

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/chengshiwen/influx-proxy/util"
	gzip "github.com/klauspost/pgzip"
)

var (
	ErrBadRequest   = errors.New("bad request")
	ErrUnauthorized = errors.New("unauthorized")
	ErrNotFound     = errors.New("not found")
	ErrInternal     = errors.New("internal error")
	ErrUnknown      = errors.New("unknown error")
)

type QueryResult struct {
	Header http.Header
	Status int
	Body   []byte
	Err    error
}

type HttpBackend struct { // nolint:golint
	client     *http.Client
	transport  *http.Transport
	interval   int
	Name       string
	Url        string // nolint:golint
	Username   string
	Password   string
	AuthSecure bool
	Active     bool
}

func NewHttpBackend(cfg *BackendConfig, pxcfg *ProxyConfig) (hb *HttpBackend) { // nolint:golint
	hb = NewSimpleHttpBackend(cfg)
	hb.client = NewClient(strings.HasPrefix(cfg.Url, "https"), pxcfg.WriteTimeout)
	hb.interval = pxcfg.CheckInterval
	go hb.CheckActive()
	return
}

func NewSimpleHttpBackend(cfg *BackendConfig) (hb *HttpBackend) { // nolint:golint
	hb = &HttpBackend{
		transport:  NewTransport(strings.HasPrefix(cfg.Url, "https")),
		Name:       cfg.Name,
		Url:        cfg.Url,
		Username:   cfg.Username,
		Password:   cfg.Password,
		AuthSecure: cfg.AuthSecure,
		Active:     true,
	}
	return
}

func NewClient(tlsSkip bool, timeout int) *http.Client {
	return &http.Client{Transport: NewTransport(tlsSkip), Timeout: time.Duration(timeout) * time.Second}
}

func NewTransport(tlsSkip bool) *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   time.Second * 30,
			KeepAlive: time.Second * 30,
		}).DialContext,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       time.Second * 90,
		TLSHandshakeTimeout:   time.Second * 10,
		ExpectContinueTimeout: time.Second * 1,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: tlsSkip},
	}
}

func NewQueryRequest(method, db, q, epoch string) *http.Request {
	header := http.Header{}
	header.Set("Accept-Encoding", "gzip")
	form := url.Values{}
	form.Set("q", q)
	if db != "" {
		form.Set("db", db)
	}
	if epoch != "" {
		form.Set("epoch", epoch)
	}
	return &http.Request{Method: method, Form: form, Header: header}
}

func CloneQueryRequest(r *http.Request) *http.Request {
	cr := r.Clone(r.Context())
	cr.Body = ioutil.NopCloser(&bytes.Buffer{})
	return cr
}

func Compress(buf *bytes.Buffer, p []byte) (err error) {
	zip := gzip.NewWriter(buf)
	defer zip.Close()
	n, err := zip.Write(p)
	if err != nil {
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		return
	}
	return
}

func CopyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Set(k, v)
		}
	}
}

func SetBasicAuth(req *http.Request, username string, password string, authSecure bool) {
	if authSecure {
		req.SetBasicAuth(util.AesDecrypt(username), util.AesDecrypt(password))
	} else {
		req.SetBasicAuth(username, password)
	}
}

func (hb *HttpBackend) SetBasicAuth(req *http.Request) {
	SetBasicAuth(req, hb.Username, hb.Password, hb.AuthSecure)
}

func (hb *HttpBackend) CheckActive() {
	for {
		hb.Active = hb.Ping()
		time.Sleep(time.Duration(hb.interval) * time.Second)
	}
}

func (hb *HttpBackend) Ping() bool {
	resp, err := hb.client.Get(hb.Url + "/ping")
	if err != nil {
		log.Print("http error: ", err)
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		log.Printf("ping status code: %d, the backend is %s", resp.StatusCode, hb.Url)
		return false
	}
	return true
}

func (hb *HttpBackend) Write(db string, p []byte) (err error) {
	var buf bytes.Buffer
	err = Compress(&buf, p)
	if err != nil {
		log.Print("compress error: ", err)
		return
	}
	return hb.WriteStream(db, &buf, true)
}

func (hb *HttpBackend) WriteCompressed(db string, p []byte) (err error) {
	buf := bytes.NewBuffer(p)
	return hb.WriteStream(db, buf, true)
}

func (hb *HttpBackend) WriteStream(db string, stream io.Reader, compressed bool) (err error) {
	q := url.Values{}
	q.Set("db", db)
	req, err := http.NewRequest("POST", hb.Url+"/write?"+q.Encode(), stream)
	if hb.Username != "" || hb.Password != "" {
		hb.SetBasicAuth(req)
	}
	if compressed {
		req.Header.Add("Content-Encoding", "gzip")
	}

	resp, err := hb.client.Do(req)
	if err != nil {
		log.Print("http error: ", err)
		hb.Active = false
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return
	}
	log.Printf("write status code: %d, from: %s", resp.StatusCode, hb.Url)

	respbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print("readall error: ", err)
		return
	}
	log.Printf("error response: %s", respbuf)

	switch resp.StatusCode {
	case 400:
		err = ErrBadRequest
	case 401:
		err = ErrUnauthorized
	case 404:
		err = ErrNotFound
	case 500:
		err = ErrInternal
	default: // mostly tcp connection timeout, or request entity too large
		err = ErrUnknown
	}
	return
}

func (hb *HttpBackend) Query(req *http.Request, w http.ResponseWriter, decompress bool) (qr *QueryResult) {
	qr = &QueryResult{}
	if len(req.Form) == 0 {
		req.Form = url.Values{}
	}
	req.Form.Del("u")
	req.Form.Del("p")
	req.ContentLength = 0
	if hb.Username != "" || hb.Password != "" {
		hb.SetBasicAuth(req)
	}

	req.URL, qr.Err = url.Parse(hb.Url + "/query?" + req.Form.Encode())
	if qr.Err != nil {
		log.Print("internal url parse error: ", qr.Err)
		return
	}

	q := strings.TrimSpace(req.FormValue("q"))
	resp, err := hb.transport.RoundTrip(req)
	if err != nil {
		if req.Header.Get("Query-Origin") != "Parallel" || err.Error() != "context canceled" {
			qr.Err = err
			log.Printf("query error: %s, the query is %s", err, q)
		}
		return
	}
	defer resp.Body.Close()
	if w != nil {
		CopyHeader(w.Header(), resp.Header)
	}

	respBody := resp.Body
	if decompress && resp.Header.Get("Content-Encoding") == "gzip" {
		var b *gzip.Reader
		b, qr.Err = gzip.NewReader(resp.Body)
		if qr.Err != nil {
			log.Printf("unable to decode gzip body")
			return
		}
		defer b.Close()
		respBody = b
	}

	qr.Body, qr.Err = ioutil.ReadAll(respBody)
	if qr.Err != nil {
		log.Printf("read body error: %s, the query is %s", qr.Err, q)
		return
	}
	if resp.StatusCode >= 400 {
		rsp, _ := ResponseFromResponseBytes(qr.Body)
		qr.Err = errors.New(rsp.Err)
	}
	qr.Header = resp.Header
	qr.Status = resp.StatusCode
	return
}

func (hb *HttpBackend) QueryIQL(method, db, q, epoch string) ([]byte, error) {
	qr := hb.Query(NewQueryRequest(method, db, q, epoch), nil, true)
	return qr.Body, qr.Err
}

func (hb *HttpBackend) GetSeriesValues(db, q string) []string {
	var values []string
	qr := hb.Query(NewQueryRequest("GET", db, q, ""), nil, true)
	if qr.Err != nil {
		return values
	}
	series, _ := SeriesFromResponseBytes(qr.Body)
	for _, s := range series {
		for _, v := range s.Values {
			if s.Name == "databases" && v[0].(string) == "_internal" {
				continue
			}
			values = append(values, v[0].(string))
		}
	}
	return values
}

// _internal has filtered
func (hb *HttpBackend) GetDatabases() []string {
	return hb.GetSeriesValues("", "show databases")
}

func (hb *HttpBackend) GetMeasurements(db string) []string {
	return hb.GetSeriesValues(db, "show measurements")
}

func (hb *HttpBackend) GetTagKeys(db, meas string) []string {
	return hb.GetSeriesValues(db, fmt.Sprintf("show tag keys from \"%s\"", util.EscapeIdentifier(meas)))
}

func (hb *HttpBackend) GetFieldKeys(db, meas string) map[string][]string {
	fieldKeys := make(map[string][]string)
	q := fmt.Sprintf("show field keys from \"%s\"", util.EscapeIdentifier(meas))
	qr := hb.Query(NewQueryRequest("GET", db, q, ""), nil, true)
	if qr.Err != nil {
		return fieldKeys
	}
	series, _ := SeriesFromResponseBytes(qr.Body)
	for _, s := range series {
		for _, v := range s.Values {
			fk := v[0].(string)
			fieldKeys[fk] = append(fieldKeys[fk], v[1].(string))
		}
	}
	return fieldKeys
}

func (hb *HttpBackend) DropMeasurement(db, meas string) ([]byte, error) {
	q := fmt.Sprintf("drop measurement \"%s\"", util.EscapeIdentifier(meas))
	qr := hb.Query(NewQueryRequest("POST", db, q, ""), nil, true)
	return qr.Body, qr.Err
}

func (hb *HttpBackend) Close() {
	hb.transport.CloseIdleConnections()
}
