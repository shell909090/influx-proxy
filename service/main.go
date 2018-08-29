// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
    "encoding/json"
    "errors"
    "flag"
    "log"
    "net/http"
    "os"
    "time"

    "gopkg.in/natefinch/lumberjack.v2"
    "gopkg.in/redis.v5"

    "github.com/chengshiwen/influx-proxy/backend"
)

var (
    ErrConfig   = errors.New("config parse error")
    ConfigFile  string
    NodeName    string
    RedisAddr   string
    LogPath     string
)

func init() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

    flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
    flag.StringVar(&NodeName, "node", "l1", "node name")
    flag.StringVar(&RedisAddr, "redis", "localhost:6379", "redis address")
    flag.StringVar(&LogPath, "log-path", "influx-proxy.log", "log file path")
    flag.Parse()
}

type Config struct {
    redis.Options
    Node string
}

func LoadJson(configfile string, cfg interface{}) (err error) {
    file, err := os.Open(configfile)
    if err != nil {
        return
    }
    defer file.Close()

    dec := json.NewDecoder(file)
    err = dec.Decode(&cfg)
    return
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
    var cfg Config

    if ConfigFile != "" {
        err = LoadJson(ConfigFile, &cfg)
        if err != nil {
            log.Print("load config failed: ", err)
            return
        }
        log.Printf("json loaded.")
    }

    if NodeName != "" {
        cfg.Node = NodeName
    }

    if RedisAddr != "" {
        cfg.Addr = RedisAddr
    }

    rcs := backend.NewRedisConfigSource(&cfg.Options, cfg.Node)

    nodecfg, err := rcs.LoadNode()
    if err != nil {
        log.Printf("config source load failed.")
        return
    }

    ic := backend.NewInfluxCluster(rcs, &nodecfg)
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
