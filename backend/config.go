// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
    "encoding/json"
    "errors"
    "log"
    "os"
)

const (
    VERSION = "1.0"
)

var (
    ErrIllegalConfig = errors.New("illegal config")
)

type NodeConfig struct {
    ListenAddr   string
    DB           string
    Zone         string
    Nexts        string
    Interval     int
    IdleTimeout  int
    WriteTracing int
    QueryTracing int
}

type BackendConfig struct {
    URL             string
    DB              string
    Zone            string
    Interval        int
    Timeout         int
    TimeoutQuery    int
    MaxRowLimit     int
    CheckInterval   int
    RewriteInterval int
    WriteOnly       int
}

type FileConfigSource struct {
    node         string
    BACKENDS     map[string]BackendConfig
    KEYMAPS      map[string][]string
    NODES        map[string]NodeConfig
    DEFAULT_NODE NodeConfig
}

func NewFileConfigSource(cfgfile string, node string) (fcs *FileConfigSource) {
    fcs = &FileConfigSource{
        node: node,
    }

    file, err := os.Open(cfgfile)
    if err != nil {
        log.Printf("file load error: %s", fcs.node)
        return
    }
    defer file.Close()
    dec := json.NewDecoder(file)
    err = dec.Decode(&fcs)
    return
}

func (fcs *FileConfigSource) LoadNode() (nodecfg NodeConfig, err error) {
    nodecfg = fcs.NODES[fcs.node]
    if nodecfg.ListenAddr == "" {
        nodecfg.ListenAddr = fcs.DEFAULT_NODE.ListenAddr
    }
    log.Printf("node config loaded.")
    return
}

func (fcs *FileConfigSource) LoadBackends() (backends map[string]*BackendConfig, err error) {
    backends = make(map[string]*BackendConfig)
    for name, val := range fcs.BACKENDS {
        cfg := &BackendConfig{
            URL: val.URL,
            DB: val.DB,
            Zone: val.Zone,
            Interval: val.Interval,
            Timeout: val.Timeout,
            TimeoutQuery: val.TimeoutQuery,
            MaxRowLimit: val.MaxRowLimit,
            CheckInterval: val.CheckInterval,
            RewriteInterval: val.RewriteInterval,
            WriteOnly: val.WriteOnly,
        }
        if cfg.Interval == 0 {
            cfg.Interval = 1000
        }
        if cfg.Timeout == 0 {
            cfg.Timeout = 10000
        }
        if cfg.TimeoutQuery == 0 {
            cfg.TimeoutQuery = 600000
        }
        if cfg.MaxRowLimit == 0 {
            cfg.MaxRowLimit = 10000
        }
        if cfg.CheckInterval == 0 {
            cfg.CheckInterval = 1000
        }
        if cfg.RewriteInterval == 0 {
            cfg.RewriteInterval = 10000
        }
        backends[name] = cfg
    }
    log.Printf("%d backends loaded from file.", len(backends))
    return
}

func (fcs *FileConfigSource) LoadMeasurements() (m_map map[string][]string, err error) {
    m_map = fcs.KEYMAPS
    log.Printf("%d measurements loaded from file.", len(m_map))
    return
}
