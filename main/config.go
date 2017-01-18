package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/shell909090/influx-proxy/backend"
	"github.com/shell909090/influx-proxy/service"
)

var (
	ErrConfig = errors.New("config parse error")
)

type Config struct {
	Addr     string
	DB       string
	Upstream map[string]backend.BackendConfig
	KeyMap   map[string][]string
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

func LoadConfig() (cfg Config, err error) {
	err = LoadJson(ConfigFile, &cfg)
	if err != nil {
		return
	}

	for _, b := range cfg.Upstream {
		if b.Timeout == 0 {
			b.Timeout = 10
		}
	}

	return
}

func (c *Config) CreateUpstreamAPI() (hs *service.HttpService, err error) {
	var upstream map[string]backend.InfluxAPI
	upstream = make(map[string]backend.InfluxAPI, 1)

	for name, bk := range c.Upstream {
		upstream[name] = bk.CreateCacheableHttp()
	}

	var key2apis map[string][]backend.InfluxAPI
	key2apis = make(map[string][]backend.InfluxAPI, 1)

	for key, ups := range c.KeyMap {
		var apis []backend.InfluxAPI
		for _, up := range ups {
			api, ok := upstream[up]
			if !ok {
				err = ErrConfig
				log.Fatal(err)
				return
			}
			apis = append(apis, api)
		}
		key2apis[key] = apis
	}

	mi := backend.NewMultiAPI(key2apis)
	hs = service.NewHttpService(mi, c.DB)
	return
}
