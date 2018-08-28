// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.
// author: ping.liu

package backend

import (
    "errors"
    "net/http"
    "regexp"
    "strings"
)

var (
    ErrNotClusterQuery = errors.New("not a cluster query")
)

type InfluxQLExecutor struct {
}

func (iqe *InfluxQLExecutor) Query(w http.ResponseWriter, req *http.Request) (err error) {
    q := strings.TrimSpace(req.FormValue("q"))
    // better way??
    matched, err := regexp.MatchString(ExecutorCmds, q)
    if err != nil || !matched {
        return ErrNotClusterQuery
    }

    w.WriteHeader(200)
    w.Write([]byte(""))

    return
}
