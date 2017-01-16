package main

import (
	"net/http"
	"net/url"
	"time"
)

type ServiceEndpoint struct {
	client *http.Client
	Url *url.URL
	DB string
}

func NewServiceEndpoint(Url string, DB string) (s *ServiceEndpoint, err error) {
	s = &ServiceEndpoint{
		client: &http.Client{
			Timeout: time.Second * 10,
		},
	}
	s.Url, err = url.Parse(Url)
	if err != nil {
		// FIXME:
		return
	}
	return
}

// TODO: cache & redo

func (s *ServiceEndpoint) Write(p []byte) (n int, err error) {
	q := url.Values{}
	q.Set("db", s.DB)

	var buf io.Reader
	buf = bytes.NewBuffer(p)
	buf, err = gzip.NewReader(buf)
	if err != nil {
		// FIXME:
		return
	}

	req, err := http.NewRequest("GET", s.Url + "?" + q.Encode(), buf)
	req.Header.Add("Content-Encoding", "gzip")

	resp, err := http.Post(, "", p)
	if err != nil {
		// FIXME:
		return
	}

	// FIXME: translate code to error
	// https://docs.influxdata.com/influxdb/v1.1/tools/api/#write
	switch resp.StatusCode {
	default:
	}
	return
}
