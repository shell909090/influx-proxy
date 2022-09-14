// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"errors"
	"strings"

	"github.com/chengshiwen/influx-proxy/util"
)

var (
	ErrGetBucket         = errors.New("can't get bucket")
	ErrEqualMeasurement  = errors.New("measurement should use ==")
	ErrMultiMeasurements = errors.New("illegal multi measurements")
	ErrIllegalFluxQuery  = errors.New("illegal flux query")
)

func ScanQuery(query string) (bucket string, measurement string, err error) {
	bucket, err = ParseBucket(query)
	if err != nil {
		return
	}
	measurement, err = ParseMeasurement(query)
	return
}

func ParseBucket(query string) (bucket string, err error) {
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

func ParseMeasurement(query string) (measurement string, err error) {
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
