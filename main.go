// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "flag"
    "github.com/chengshiwen/influx-proxy/consist"
    "github.com/chengshiwen/influx-proxy/mconst"
    "github.com/chengshiwen/influx-proxy/service"
    "net/http"
    "time"
)

var (
    ConsistentFILE  string
)

func main() {
    flag.StringVar(&ConsistentFILE, "proxy", "proxy.json", "proxy file")
    flag.Parse()
    proxy := consist.NewProxy(ConsistentFILE)
    httpServer := service.HttpService{Proxy: proxy}
    mux := http.NewServeMux()
    httpServer.Register(mux)

    server := &http.Server{
        Addr:        proxy.ListenAddr,
        Handler:     mux,
        IdleTimeout: mconst.IdleTimeOut * time.Second,
    }
    err := server.ListenAndServe()
    if err != nil {
        panic(err)
    }
}
