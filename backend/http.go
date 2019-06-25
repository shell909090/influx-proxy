// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

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
	"strings"
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
	Interval  int
	URL       string
	DB        string
	Zone      string
	Active    bool
	running   bool
	WriteOnly int
}

func NewHttpBackend(cfg *BackendConfig) (hb *HttpBackend) {
	hb = &HttpBackend{
		client: &http.Client{
			Timeout: time.Millisecond * time.Duration(cfg.Timeout),
		},
		// TODO: query timeout? use req.Cancel
		// client_query: &http.Client{
		// 	Timeout: time.Millisecond * time.Duration(cfg.TimeoutQuery),
		// },
		Interval:  cfg.CheckInterval,
		URL:       cfg.URL,
		DB:        cfg.DB,
		Zone:      cfg.Zone,
		Active:    true,
		running:   true,
		WriteOnly: cfg.WriteOnly,
	}
	go hb.CheckActive()
	return
}

// TODO: update active when calling successed or failed.

func (hb *HttpBackend) CheckActive() {
	var err error
	for hb.running {
		_, err = hb.Ping()
		hb.Active = (err == nil)
		time.Sleep(time.Millisecond * time.Duration(hb.Interval))
	}
}

func (hb *HttpBackend) IsWriteOnly() bool {
	if hb.WriteOnly == 0 {
		return false
	}
	return true
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

	if resp.StatusCode == 204 {
		return
	}
	log.Printf("write status code: %d, the backend is %s\n", resp.StatusCode, hb.URL)

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
	if len(req.Form) == 0 {
		req.Form = url.Values{}
	}
	req.Form.Set("db", hb.DB)
	req.ContentLength = 0

	req.URL, err = url.Parse(hb.URL + "/query?" + req.Form.Encode())
	if err != nil {
		log.Print("internal url parse error: ", err)
		return
	}

	q := strings.TrimSpace(req.FormValue("q"))
	resp, err := hb.transport.RoundTrip(req)
	if err != nil {
		log.Printf("query error: %s,the query is %s\n", err, q)
		hb.Active = false
		return
	}
	defer resp.Body.Close()

	copyHeader(w.Header(), resp.Header)

	p, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("read body error: %s,the query is %s\n", err, q)
		return
	}

	w.WriteHeader(resp.StatusCode)
	w.Write(p)
	return
}

func ScanParams(pointbuf []byte) (params string, point []byte) {
	buflen := len(pointbuf)

	for i := 0; i < buflen; i++ {
		if pointbuf[i] == '|' {
			return string(pointbuf[:i]), pointbuf[i+1:]
		}
	}
	return "", pointbuf
}

func (hb *HttpBackend) Write(p []byte) (err error) {
	buf := bytes.NewBuffer(p)
	bufMap := make(map[string]*bytes.Buffer) 

	for {
		line, _ := buf.ReadBytes('\n')
		if len(line) ==0 {
			break
		}
		optionParams, point:= ScanParams(line)
		
		if (bufMap[optionParams] == nil) {
			bufMap[optionParams] = &bytes.Buffer{}
		}
		bufMap[optionParams].Write(point)
	}

	for optionParams, buffer := range(bufMap) {
		p := buffer.Bytes()
		var buf bytes.Buffer
		err := Compress(&buf, p)
		if err != nil {
			log.Printf("write file error: %s\n", err)
			continue
		}
		err = hb.WriteStream(&buf, true, optionParams)
	}
	return
}


func (hb *HttpBackend) WriteCompressed(p []byte) (err error) {
	buf := bytes.NewBuffer(p)
	err = hb.WriteStream(buf, true, "")
	return
}

func parseParamString(optionParams string) (result map[string]string) {
	result = make(map[string]string)

	entrys := strings.Split(optionParams, "&")
	
	for _, entry := range(entrys){
	   pair := strings.Split(entry, "=")
	   if pair[1] != "" {
	       result[pair[0]] = pair[1]
	   }
	}
	return
}

func (hb *HttpBackend) WriteStream(stream io.Reader, compressed bool, optionParams string) (err error) {
	q := url.Values{}
	q.Set("db", hb.DB)

	if (optionParams != "") {
		paramsObject := parseParamString(optionParams)
		for key, value := range(paramsObject){
			q.Set(key, value)
		}
	}

	req, err := http.NewRequest("POST", hb.URL+"/write?"+q.Encode(), stream)
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
	log.Print("write status code: ", resp.StatusCode)

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
	default: // mostly tcp connection timeout
		log.Printf("status: %d", resp.StatusCode)
		err = ErrUnknown
	}
	return
}

func (hb *HttpBackend) Close() (err error) {
	hb.running = false
	hb.transport.CloseIdleConnections()
	return
}
