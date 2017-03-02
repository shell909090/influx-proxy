// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: ping.liu

package backend

import (
	"errors"
	"net/http"
)

var (
	ErrNotClusterQuery = errors.New("not a cluster query")
)

type InfluxQLExecutor struct {
}

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request) (err error) {
	return ErrNotClusterQuery
}
