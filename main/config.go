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

type Backend struct {
	URL      string
	DB       string
	Interval int
}

type Config struct {
	Upstream map[string]Backend
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

	return
}

func (c *Config) CreateUpstreamAPI() (hs *service.HttpService, err error) {
	var upstream map[string]backend.InfluxAPI
	upstream = make(map[string]backend.InfluxAPI, 1)

	var hb *backend.HttpBackend
	for name, bk := range c.Upstream {
		hb, err = backend.NewHttpBackend(bk.URL, bk.DB)
		if err != nil {
			log.Printf("error: %s", err)
			return
		}

		ca := backend.NewCacheableAPI(hb, bk.Interval)
		upstream[name] = ca
	}

	var key2apis map[string][]backend.InfluxAPI
	key2apis = make(map[string][]backend.InfluxAPI, 1)

	for key, ups := range c.KeyMap {
		var apis []backend.InfluxAPI
		for _, up := range ups {
			api, ok := upstream[up]
			if !ok {
				err = ErrConfig
				log.Fatalf("error: %s", err)
				return
			}
			apis = append(apis, api)
		}
		key2apis[key] = apis
	}

	mi := backend.NewMultiAPI(key2apis)
	hs = service.NewHttpService(mi, "wtf")
	return
}
