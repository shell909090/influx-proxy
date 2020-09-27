package backend

import (
	"bytes"

	"github.com/influxdata/influxdb1-client/models"
	jsoniter "github.com/json-iterator/go"
)

// Message represents a user-facing message to be included with the result.
type Message struct {
	Level string `json:"level"`
	Text  string `json:"text"`
}

// Result represents a resultset returned from a single statement.
// Rows represents a list of rows that can be sorted consistently by name/tag.
type Result struct {
	// StatementID is just the statement's position in the query. It's used
	// to combine statement results if they're being buffered in memory.
	StatementID int         `json:"statement_id"`
	Series      models.Rows `json:"series,omitempty"`
	Messages    []*Message  `json:"messages,omitempty"`
	Partial     bool        `json:"partial,omitempty"`
	Err         string      `json:"error,omitempty"`
}

// Response represents a list of statement results.
type Response struct {
	Results []*Result `json:"results,omitempty"`
	Err     string    `json:"error,omitempty"`
}

func (rsp *Response) Unmarshal(b []byte) (e error) {
	dec := jsoniter.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(rsp)
}

func SeriesFromResponseBytes(b []byte) (series models.Rows, e error) {
	rsp := &Response{}
	e = rsp.Unmarshal(b)
	if e == nil && len(rsp.Results) > 0 && len(rsp.Results[0].Series) > 0 {
		series = rsp.Results[0].Series
	}
	return
}

func ResultsFromResponseBytes(b []byte) (results []*Result, e error) {
	rsp := &Response{}
	e = rsp.Unmarshal(b)
	if e == nil && len(rsp.Results) > 0 {
		results = rsp.Results
	}
	return
}

func ResponseFromResponseBytes(b []byte) (rsp *Response, e error) {
	rsp = &Response{}
	e = rsp.Unmarshal(b)
	return
}

func ResponseFromSeries(series models.Rows) (rsp *Response) {
	r := &Result{
		Series: series,
	}
	rsp = &Response{
		Results: []*Result{r},
	}
	return
}

func ResponseFromResults(results []*Result) (rsp *Response) {
	rsp = &Response{
		Results: results,
	}
	return
}

func ResponseFromError(err string) (rsp *Response) {
	rsp = &Response{
		Err: err,
	}
	return
}
