// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "flag"
    "fmt"
    "github.com/chengshiwen/influx-proxy/consistent"
    "github.com/chengshiwen/influx-proxy/service"
    "github.com/chengshiwen/influx-proxy/util"
    "net/http"
    "runtime"
    "time"
)

var (
    ConfigFile      string
    Version         bool
    GitCommit       string
    BuildTime       string
)

func main() {
    flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
    flag.BoolVar(&Version, "version", false, "proxy version")
    flag.Parse()
    if Version {
        fmt.Printf("Version:    %s\n", util.Version)
        fmt.Printf("Git commit: %s\n", GitCommit)
        fmt.Printf("Go version: %s\n", runtime.Version())
        fmt.Printf("Build time: %s\n", BuildTime)
        fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
        return
    }

    proxy := consistent.NewProxy(ConfigFile)
    hs := service.HttpService{Proxy: proxy}
    mux := http.NewServeMux()
    hs.Register(mux)

    server := &http.Server{
        Addr:        proxy.ListenAddr,
        Handler:     mux,
        IdleTimeout: util.IdleTimeOut * time.Second,
    }
    err := server.ListenAndServe()
    if err != nil {
        panic(err)
    }
}
