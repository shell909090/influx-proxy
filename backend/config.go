package backend

import (
	"encoding/json"
	"errors"
	"github.com/deckarep/golang-set"
	"log"
	"os"
)

const (
	VERSION = "2.5.0"
)

var (
	ErrEmptyCircles          = errors.New("circles cannot be empty")
	ErrEmptyBackends         = errors.New("backends cannot be empty")
	ErrEmptyBackendName      = errors.New("backend name cannot be empty")
	ErrDuplicatedBackendName = errors.New("backend name duplicated")
	ErrInvalidHashKey        = errors.New("invalid hash_key, should be idx, name or url")
)

type BackendConfig struct {
	Name       string `json:"name"`
	Url        string `json:"url"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	AuthSecure bool   `json:"auth_secure"`
}

type CircleConfig struct {
	Name     string           `json:"name"`
	Backends []*BackendConfig `json:"backends"`
}

type ProxyConfig struct {
	Circles         []*CircleConfig `json:"circles"`
	ListenAddr      string          `json:"listen_addr"`
	DbList          []string        `json:"db_list"`
	DataDir         string          `json:"data_dir"`
	TLogDir         string          `json:"tlog_dir"`
	HashKey         string          `json:"hash_key"`
	VNodeSize       int             `json:"vnode_size"`
	FlushSize       int             `json:"flush_size"`
	FlushTime       int             `json:"flush_time"`
	CheckInterval   int             `json:"check_interval"`
	RewriteInterval int             `json:"rewrite_interval"`
	WriteTimeout    int             `json:"write_timeout"`
	IdleTimeout     int             `json:"idle_timeout"`
	ConnPoolSize    int             `json:"conn_pool_size"`
	LogEnabled      bool            `json:"log_enabled"`
	Username        string          `json:"username"`
	Password        string          `json:"password"`
	AuthSecure      bool            `json:"auth_secure"`
	HTTPSEnabled    bool            `json:"https_enabled"`
	HTTPSCert       string          `json:"https_cert"`
	HTTPSKey        string          `json:"https_key"`
}

func NewFileConfig(cfgfile string) (cfg *ProxyConfig, err error) {
	cfg = &ProxyConfig{}
	file, err := os.Open(cfgfile)
	if err != nil {
		return
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	err = dec.Decode(cfg)
	if err != nil {
		return
	}
	cfg.setDefault()
	err = cfg.checkConfig()
	if err != nil {
		return
	}
	return
}

func (cfg *ProxyConfig) setDefault() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":7076"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if cfg.TLogDir == "" {
		cfg.TLogDir = "log"
	}
	if cfg.HashKey == "" {
		cfg.HashKey = "idx"
	}
	if cfg.VNodeSize <= 0 {
		cfg.VNodeSize = 256
	}
	if cfg.FlushSize <= 0 {
		cfg.FlushSize = 10000
	}
	if cfg.FlushTime <= 0 {
		cfg.FlushTime = 1
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 1
	}
	if cfg.RewriteInterval <= 0 {
		cfg.RewriteInterval = 10
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 10
	}
	if cfg.ConnPoolSize <= 0 {
		cfg.ConnPoolSize = 20
	}
}

func (cfg *ProxyConfig) checkConfig() (err error) {
	if len(cfg.Circles) == 0 {
		return ErrEmptyCircles
	}
	rec := mapset.NewSet()
	for _, circle := range cfg.Circles {
		if len(circle.Backends) == 0 {
			return ErrEmptyBackends
		}
		for _, backend := range circle.Backends {
			if backend.Name == "" {
				return ErrEmptyBackendName
			}
			if rec.Contains(backend.Name) {
				return ErrDuplicatedBackendName
			}
			rec.Add(backend.Name)
		}
	}
	if cfg.HashKey != "idx" && cfg.HashKey != "name" && cfg.HashKey != "url" {
		return ErrInvalidHashKey
	}
	return
}

func (cfg *ProxyConfig) PrintSummary() {
	log.Printf("%d circles loaded from file", len(cfg.Circles))
	for id, circle := range cfg.Circles {
		log.Printf("circle %d: %d backends loaded", id, len(circle.Backends))
	}
	log.Printf("hash key: %s", cfg.HashKey)
	if len(cfg.DbList) > 0 {
		log.Printf("db list: %v", cfg.DbList)
	}
}
