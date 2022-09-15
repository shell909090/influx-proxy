// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"github.com/chengshiwen/influx-proxy/util"
)

var (
	ErrGetBucket         = errors.New("can't get bucket")
	ErrEqualMeasurement  = errors.New("measurement must use ==")
	ErrMultiMeasurements = errors.New("illegal multi measurements")
	ErrIllegalFluxQuery  = errors.New("illegal flux query")
)

type QueryRequest struct {
	Spec  *Spec  `json:"spec,omitempty"`
	Query string `json:"query"`
	Type  string `json:"type"`
}

type Spec struct {
	Operations []*Operation `json:"operations"`
}

func (s *Spec) String() string {
	b, _ := json.Marshal(s)
	return string(b)
}

type Operation struct {
	Kind string          `json:"kind"`
	Spec json.RawMessage `json:"spec"`
}

type FilterBody struct {
	Type     string   `json:"type"`
	Operator string   `json:"operator"`
	Left     *Literal `json:"left"`
	Right    *Literal `json:"right"`
}

type Literal struct {
	Type     string `json:"type"`
	Value    string `json:"value,omitempty"`
	Property string `json:"property,omitempty"`
}

func ScanQuery(query string) (bucket string, measurement string, err error) {
	bucket, err = ParseQueryBucket(query)
	if err != nil {
		return
	}
	measurement, err = ParseQueryMeasurement(query)
	return
}

func ParseQueryBucket(query string) (bucket string, err error) {
	i := strings.Index(query, "from")
	if i == -1 || i >= len(query)-4 {
		err = ErrGetBucket
		return
	}
	str := query[i+4:]
	j := strings.Index(str, ")")
	if j == -1 {
		err = ErrIllegalFluxQuery
		return
	}
	items := strings.Split(strings.Trim(str[:j+1], " ()"), ",")
	for _, item := range items {
		entries := strings.Split(strings.TrimSpace(item), ":")
		if len(entries) == 2 && entries[0] == "bucket" {
			bucket = strings.Trim(entries[1], " \"'")
			return
		}
	}
	return "", ErrGetBucket
}

func ParseQueryMeasurement(query string) (measurement string, err error) {
	items := strings.Split(query, "._measurement")
	if len(items) < 2 {
		items = strings.Split(query, `["_measurement"]`)
		if len(items) < 2 {
			err = ErrGetMeasurement
			return
		}
	}
	if len(items) > 2 {
		err = ErrMultiMeasurements
		return
	}
	str := strings.TrimSpace(items[1])
	if !strings.HasPrefix(str, "==") {
		err = ErrEqualMeasurement
		return
	}
	str = strings.TrimLeft(str, "= ")
	if str[0] == '"' {
		advance, _, err := FindEndWithQuote([]byte(str), 0, '"')
		if err != nil {
			return "", err
		}
		return util.UnescapeIdentifier(str[1 : advance-1]), nil
	}
	return "", ErrGetMeasurement
}

func ScanSpec(spec *Spec) (bucket string, measurement string, err error) {
	for _, op := range spec.Operations {
		switch op.Kind {
		case "influxDBFrom":
			bucket, err = ParseSpecBucket([]byte(op.Spec))
		case "filter":
			measurement, err = ParseSpecMeasurement([]byte(op.Spec))
		}
		if err != nil {
			return
		}
	}
	return
}

func ParseSpecBucket(spec []byte) (bucket string, err error) {
	var m map[string]string
	err = json.Unmarshal(spec, &m)
	if err != nil {
		return
	}
	bucket, ok := m["bucket"]
	if ok {
		return
	}
	return "", ErrGetBucket
}

func ParseSpecMeasurement(spec []byte) (measurement string, err error) {
	i := bytes.Index(spec, []byte(`"body"`))
	if i < 0 || i+7 >= len(spec) {
		return "", ErrGetMeasurement
	}
	for ; i < len(spec); i++ {
		if spec[i] == '{' {
			break
		}
	}
	if i == len(spec) {
		return "", ErrGetMeasurement
	}
	j, bracket := i, 0
	for ; j < len(spec); j++ {
		if spec[j] == '{' {
			bracket++
		} else if spec[j] == '}' {
			bracket--
		}
		if bracket <= 0 {
			break
		}
	}
	if bracket == 0 {
		body := spec[i : j+1]
		var fb FilterBody
		err = json.Unmarshal(body, &fb)
		if err != nil {
			return
		}
		if fb.Operator == "" || fb.Left == nil || fb.Right == nil {
			return "", ErrGetMeasurement
		}
		if fb.Operator != "==" {
			return "", ErrEqualMeasurement
		}
		if fb.Left.Property == "_measurement" && fb.Right.Value != "" {
			measurement = fb.Right.Value
			return
		}
	}
	return "", ErrGetMeasurement
}
