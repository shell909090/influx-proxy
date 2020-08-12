package backend

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/chengshiwen/influx-proxy/util"
	gzip "github.com/klauspost/pgzip"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var (
	ErrBadRequest   = errors.New("bad request")
	ErrUnauthorized = errors.New("unauthorized")
	ErrNotFound     = errors.New("not found")
	ErrInternal     = errors.New("internal error")
	ErrUnknown      = errors.New("unknown error")
)

type HttpBackend struct {
	client     *http.Client
	transport  *http.Transport
	interval   int
	Name       string
	Url        string
	Username   string
	Password   string
	AuthSecure bool
	Active     bool
}

func NewHttpBackend(cfg *BackendConfig, pxcfg *ProxyConfig) (hb *HttpBackend) {
	hb = NewSimpleHttpBackend(cfg)
	hb.client = NewClient(strings.HasPrefix(cfg.Url, "https"), pxcfg.WriteTimeout)
	hb.interval = pxcfg.CheckInterval
	go hb.CheckActive()
	return
}

func NewSimpleHttpBackend(cfg *BackendConfig) (hb *HttpBackend) {
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

func NewTransport(tlsSkip bool) (transport *http.Transport) {
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

func NewRequest(db, query string) *http.Request {
	header := map[string][]string{"Accept-Encoding": {"gzip"}}
	if db == "" {
		return &http.Request{Form: url.Values{"q": []string{query}}, Header: header}
	}
	return &http.Request{Form: url.Values{"db": []string{db}, "q": []string{query}}, Header: header}
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

func (hb *HttpBackend) WriteCompressed(db string, p []byte) error {
	buf := bytes.NewBuffer(p)
	return hb.WriteStream(db, buf, true)
}

func (hb *HttpBackend) WriteStream(db string, stream io.Reader, compressed bool) error {
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
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 204 {
		return nil
	}
	log.Printf("write status code: %d, from: %s", resp.StatusCode, hb.Url)

	respbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print("readall error: ", err)
		return err
	}
	log.Printf("error response: %s", respbuf)

	switch resp.StatusCode {
	case 400:
		return ErrBadRequest
	case 401:
		return ErrUnauthorized
	case 404:
		return ErrNotFound
	case 500:
		return ErrInternal
	default: // mostly tcp connection timeout, or request entity too large
		return ErrUnknown
	}
}

func (hb *HttpBackend) Query(req *http.Request, w http.ResponseWriter, decompressed bool) ([]byte, error) {
	var err error
	if len(req.Form) == 0 {
		req.Form = url.Values{}
	}
	req.Form.Del("u")
	req.Form.Del("p")
	req.ContentLength = 0
	if hb.Username != "" || hb.Password != "" {
		hb.SetBasicAuth(req)
	}

	req.URL, err = url.Parse(hb.Url + "/query?" + req.Form.Encode())
	if err != nil {
		log.Print("internal url parse error: ", err)
		return nil, err
	}

	q := strings.TrimSpace(req.FormValue("q"))
	resp, err := hb.transport.RoundTrip(req)
	if err != nil {
		log.Printf("query error: %s, the query is %s", err, q)
		return nil, err
	}
	defer resp.Body.Close()
	if w != nil {
		CopyHeader(w.Header(), resp.Header)
	}

	body := resp.Body
	if decompressed && resp.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(resp.Body)
		defer b.Close()
		if err != nil {
			log.Printf("unable to decode gzip body")
			return nil, err
		}
		body = b
	}

	return ioutil.ReadAll(body)
}

func (hb *HttpBackend) QueryIQL(db, query string) ([]byte, error) {
	return hb.Query(NewRequest(db, query), nil, true)
}

func (hb *HttpBackend) GetSeriesValues(db, query string) []string {
	var values []string
	p, err := hb.Query(NewRequest(db, query), nil, true)
	if err != nil {
		return values
	}
	series, _ := SeriesFromResponseBytes(p)
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
	query := fmt.Sprintf("show field keys from \"%s\"", util.EscapeIdentifier(meas))
	p, err := hb.Query(NewRequest(db, query), nil, true)
	if err != nil {
		return fieldKeys
	}
	series, _ := SeriesFromResponseBytes(p)
	for _, s := range series {
		for _, v := range s.Values {
			fk := v[0].(string)
			fieldKeys[fk] = append(fieldKeys[fk], v[1].(string))
		}
	}
	return fieldKeys
}

func (hb *HttpBackend) DropMeasurement(db, meas string) ([]byte, error) {
	query := fmt.Sprintf("drop measurement \"%s\"", util.EscapeIdentifier(meas))
	return hb.Query(NewRequest(db, query), nil, true)
}

func (hb *HttpBackend) Close() {
	hb.client.CloseIdleConnections()
	hb.transport.CloseIdleConnections()
}
