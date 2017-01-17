package backend

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"
)

type HttpBackend struct {
	client *http.Client
	Url    string
	DB     string
}

func NewHttpBackend(Url string, DB string) (hb *HttpBackend, err error) {
	hb = &HttpBackend{
		client: &http.Client{
			Timeout: time.Second * 10,
		},
		Url: Url,
		DB:  DB,
	}

	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	return
}

func (hb *HttpBackend) Write(p []byte) (err error) {
	// ignore db
	q := url.Values{}
	q.Set("db", hb.DB)

	fmt.Printf("debug %s: %s\n", hb.DB, string(p))

	var buf bytes.Buffer
	zip := gzip.NewWriter(&buf)
	n, err := zip.Write(p)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	if n != len(p) {
		err = io.ErrShortWrite
		log.Printf("error: %s\n", err)
		return
	}
	err = zip.Close()
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	req, err := http.NewRequest("POST", hb.Url+"/write?"+q.Encode(), &buf)
	req.Header.Add("Content-Encoding", "gzip")

	resp, err := hb.client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}

	fmt.Printf("status code: %d\n", resp.StatusCode)
	if resp.StatusCode == 204 {
		return
	}

	respbuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error: %s\n", err)
		return
	}
	log.Printf("error response: %s\n", respbuf[:n])

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
