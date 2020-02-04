// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "flag"
    "fmt"
    "github.com/chengshiwen/influx-proxy/consist"
    "github.com/chengshiwen/influx-proxy/mconst"
    "github.com/chengshiwen/influx-proxy/service"
    "net/http"
    "runtime"
    "time"
)

var (
    ProxyFile       string
    Version         bool
    GitCommit       string
    BuildTime       string
)

func main() {
    flag.StringVar(&ProxyFile, "proxy", "proxy.json", "proxy config file")
    flag.BoolVar(&Version, "version", false, "proxy version")
    flag.Parse()
    if Version {
        fmt.Printf("Version:    %s\n", mconst.Version)
        fmt.Printf("Git commit: %s\n", GitCommit)
        fmt.Printf("Go version: %s\n", runtime.Version())
        fmt.Printf("Build time: %s\n", BuildTime)
        fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
        return
    }

    proxy := consist.NewProxy(ProxyFile)
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
