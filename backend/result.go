package backend

import (
    "github.com/influxdata/influxdb1-client/models"
    "github.com/json-iterator/go"
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

func SeriesFromResponseBytes(b []byte) (series models.Rows, e error) {
    var rsp Response
    e = jsoniter.Unmarshal(b, &rsp)
    if e == nil && len(rsp.Results) > 0 && len(rsp.Results[0].Series) > 0 {
        series = rsp.Results[0].Series
    }
    return
}

func ResponseBytesFromSeries(series models.Rows) (b []byte, e error) {
    r := &Result{
        Series: series,
    }
    rsp := Response{
        Results: []*Result{r},
    }
    b, e = jsoniter.Marshal(rsp)
    if e != nil {
        return
    }
    b = append(b, '\n')
    return
}

func ResultsFromResponseBytes(b []byte) (results []*Result, e error) {
    var rsp Response
    e = jsoniter.Unmarshal(b, &rsp)
    if e == nil && len(rsp.Results) > 0 {
        results = rsp.Results
    }
    return
}

func ResponseBytesFromResults(results []*Result) (b []byte, e error) {
    rsp := Response{
        Results: results,
    }
    b, e = jsoniter.Marshal(rsp)
    if e != nil {
        return
    }
    b = append(b, '\n')
    return
}
