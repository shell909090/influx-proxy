package main

import (
	"flag"
	"log"
	"net/http"
)

var (
	ConfigFile string
)

func init() {
	flag.StringVar(&ConfigFile, "config", "/etc/influxdb/proxy.json", "config file")
	flag.Parse()
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Print(err)
		return
	}

	h, err := cfg.CreateUpstreamAPI()
	if err != nil {
		log.Print(err)
		return
	}

	mux := http.NewServeMux()
	h.Register(mux)
	for {
		err = http.ListenAndServe(addr, handler)
		if err != nil {
			log.Error("%s", err.Error())
			return
		}
	}
}
