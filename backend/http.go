package backend

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
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
	client       *http.Client
	client_query *http.Client
	URL          string
	DB           string
}

func NewHttpBackend(cfg *BackendConfig) (hb *HttpBackend) {
	hb = &HttpBackend{
		client: &http.Client{
			Timeout: time.Second * time.Duration(cfg.Timeout),
		},
		client_query: &http.Client{
			Timeout: time.Second * time.Duration(cfg.TimeoutQuery),
		},
		URL: cfg.URL,
		DB:  cfg.DB,
	}
	return
}

func (hb *HttpBackend) Ping() (version string, err error) {
	return
}

func (hb *HttpBackend) Write(p []byte) (err error) {
	q := url.Values{}
	q.Set("db", hb.DB)

	log.Printf("http backend write %s\n%s\n", hb.DB, string(p))

	var buf bytes.Buffer
	err = Compress(&buf, p)
	if err != nil {
		log.Print("compress error: ", err)
		return
	}

	req, err := http.NewRequest("POST", hb.URL+"/write?"+q.Encode(), &buf)
	req.Header.Add("Content-Encoding", "gzip")

	resp, err := hb.client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	log.Print("status code: ", resp.StatusCode)
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
