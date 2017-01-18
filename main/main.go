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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&ConfigFile, "config", "/etc/influxdb/proxy.json", "config file")
	flag.Parse()
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Print("load config failed: ", err)
		return
	}

	h, err := cfg.CreateUpstreamAPI()
	if err != nil {
		return
	}

	mux := http.NewServeMux()
	h.Register(mux)

	err = http.ListenAndServe(cfg.Addr, mux)
	if err != nil {
		log.Print(err)
		return
	}
}
