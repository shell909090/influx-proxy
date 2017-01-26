package backend

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

var (
	ErrBadRequest = errors.New("Bad Request")
	ErrNotFound   = errors.New("Not Found")
	ErrInternal   = errors.New("Internal Error")
	ErrUnknown    = errors.New("Unknown Error")
)

func Compress(buf *bytes.Buffer, p []byte) (err error) {
	zip := gzip.NewWriter(buf)
	n, err := zip.Write(p)
	if err != nil {
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		return
	}
	err = zip.Close()
	return
}

type HttpBackend struct {
	client    *http.Client
	transport http.Transport
	Timeout   int
	URL       string
	DB        string
	Zone      string
	Active    bool
	running   bool
}

func NewHttpBackend(cfg *BackendConfig) (hb *HttpBackend) {
	hb = &HttpBackend{
		client: &http.Client{
			Timeout: time.Second * time.Duration(cfg.Timeout),
		},
		// TODO: query timeout? use req.Cancel
		// client_query: &http.Client{
		// 	Timeout: time.Second * time.Duration(cfg.TimeoutQuery),
		// },
		Timeout: cfg.Timeout,
		URL:     cfg.URL,
		DB:      cfg.DB,
		Zone:    cfg.Zone,
		Active:  true,
		running: true,
	}
	go hb.CheckActive()
	return
}

func (hb *HttpBackend) CheckActive() {
	var err error
	for hb.running {
		_, err = hb.Ping()
		hb.Active = (err == nil)
		time.Sleep(time.Second * time.Duration(hb.Timeout))
	}
}

func (hb *HttpBackend) IsActive() bool {
	return hb.Active
}

func (hb *HttpBackend) Ping() (version string, err error) {
	resp, err := hb.client.Get(hb.URL + "/ping")
	if err != nil {
		log.Print("http error: ", err)
		return
	}
	defer resp.Body.Close()

	version = resp.Header.Get("X-Influxdb-Version")

	log.Print("ping status code: ", resp.StatusCode)
	if resp.StatusCode == 204 {
		return
	}

	respbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print("readall error: ", err)
		return
	}
	log.Printf("error response: %s\n", respbuf)
	return
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func (hb *HttpBackend) GetZone() (zone string) {
	return hb.Zone
}

// Don't setup Accept-Encoding: gzip. Let real client do so.
// If real client don't support gzip and we setted, it will be a mistake.
func (hb *HttpBackend) Query(w http.ResponseWriter, req *http.Request) (err error) {
	q := req.URL.Query()
	q.Set("db", hb.DB)
	req.URL, err = url.Parse(hb.URL + "/query?" + q.Encode())
	if err != nil {
		log.Print("internal url parse error: ", err)
		w.WriteHeader(400)
		w.Write([]byte("internal url parse error"))
		return
	}

	resp, err := hb.transport.RoundTrip(req)
	if err != nil {
		log.Print("query error: ", err)
		w.WriteHeader(400)
		w.Write([]byte("query error"))
		hb.Active = false
		return
	}
	defer resp.Body.Close()

	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		log.Print("copy body error: ", err)
		w.WriteHeader(400)
		w.Write([]byte("copy body error"))
		return
	}
	return
}

func (hb *HttpBackend) Write(p []byte) (err error) {
	var buf bytes.Buffer
	err = Compress(&buf, p)
	if err != nil {
		log.Print("compress error: ", err)
		return
	}

	log.Printf("http backend write %s", hb.DB)
	err = hb.WriteStream(&buf, true)
	return
}

func (hb *HttpBackend) WriteStream(stream io.Reader, compressed bool) (err error) {
	q := url.Values{}
	q.Set("db", hb.DB)

	req, err := http.NewRequest("POST", hb.URL+"/write?"+q.Encode(), stream)
	if compressed {
		req.Header.Add("Content-Encoding", "gzip")
	}

	resp, err := hb.client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		log.Print("http error: ", err)
		hb.Active = false
		return
	}

	log.Print("write status code: ", resp.StatusCode)
	if resp.StatusCode == 204 {
		return
	}

	respbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Print("readall error: ", err)
		return
	}
	log.Printf("error response: %s\n", respbuf)

	// translate code to error
	// https://docs.influxdata.com/influxdb/v1.1/tools/api/#write
	switch resp.StatusCode {
	case 400:
		err = ErrBadRequest
	case 404:
		err = ErrNotFound
	default:
		err = ErrUnknown
	}
	return
}

func (hb *HttpBackend) Close() (err error) {
	hb.running = false
	return
}
