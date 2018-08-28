// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "errors"
    "flag"
    "log"
    "net/http"
    "os"
    "time"

    "gopkg.in/natefinch/lumberjack.v2"

    "github.com/chengshiwen/influx-proxy/backend"
)

var (
    ErrConfig   = errors.New("config parse error")
    ConfigFile  string
    NodeName    string
    LogPath     string
)

func init() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

    flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
    flag.StringVar(&NodeName, "node", "l1", "node name")
    flag.StringVar(&LogPath, "log-path", "influx-proxy.log", "log file path")
    flag.Parse()
}

func initLog() {
    if LogPath == "" {
        log.SetOutput(os.Stdout)
    } else {
        log.SetOutput(&lumberjack.Logger{
            Filename:   LogPath,
            MaxSize:    100,
            MaxBackups: 5,
            MaxAge:     7,
        })
    }
}

func main() {
    initLog()

    var err error

    fcs := backend.NewFileConfigSource(ConfigFile, NodeName)

    nodecfg, err := fcs.LoadNode()
    if err != nil {
        log.Printf("config source load failed.")
        return
    }

    ic := backend.NewInfluxCluster(fcs, &nodecfg)
    ic.LoadConfig()

    mux := http.NewServeMux()
    NewHttpService(ic, nodecfg.DB).Register(mux)

    log.Printf("http service start.")
    server := &http.Server{
        Addr:        nodecfg.ListenAddr,
        Handler:     mux,
        IdleTimeout: time.Duration(nodecfg.IdleTimeout) * time.Second,
    }
    if nodecfg.IdleTimeout <= 0 {
        server.IdleTimeout = 10 * time.Second
    }
    err = server.ListenAndServe()
    if err != nil {
        log.Print(err)
        return
    }
}
