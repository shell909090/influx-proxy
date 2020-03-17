// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "flag"
    "fmt"
    "github.com/chengshiwen/influx-proxy/backend"
    "github.com/chengshiwen/influx-proxy/config"
    "github.com/chengshiwen/influx-proxy/service"
    "log"
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
        fmt.Printf("Version:    %s\n", config.Version)
        fmt.Printf("Git commit: %s\n", GitCommit)
        fmt.Printf("Go version: %s\n", runtime.Version())
        fmt.Printf("Build time: %s\n", BuildTime)
        fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
        return
    }

    proxy, err := backend.NewProxy(ConfigFile)
    if err != nil {
        fmt.Println("config source load failed:", err.Error())
        return
    }
    hs := service.HttpService{Proxy: proxy}
    mux := http.NewServeMux()
    hs.Register(mux)

    server := &http.Server{
        Addr:        proxy.ListenAddr,
        Handler:     mux,
        IdleTimeout: config.IdleTimeout * time.Second,
    }
    if proxy.HTTPSEnabled {
        err = server.ListenAndServeTLS(proxy.HTTPSCert, proxy.HTTPSKey)
    } else {
        err = server.ListenAndServe()
    }
    if err != nil {
        log.Print(err)
        return
    }
}
