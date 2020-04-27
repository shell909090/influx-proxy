package main

import (
	"flag"
	"fmt"
	"github.com/chengshiwen/influx-proxy/backend"
	"github.com/chengshiwen/influx-proxy/service"
	"log"
	"net/http"
	"runtime"
	"time"
)

var (
	ConfigFile string
	Version    bool
	GitCommit  string
	BuildTime  string
)

func main() {
	flag.StringVar(&ConfigFile, "config", "proxy.json", "proxy config file")
	flag.BoolVar(&Version, "version", false, "proxy version")
	flag.Parse()
	if Version {
		fmt.Printf("Version:    %s\n", service.Version)
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
		IdleTimeout: time.Duration(proxy.IdleTimeout) * time.Second,
	}
	if proxy.HTTPSEnabled {
		log.Printf("https service start, listen on %s", server.Addr)
		err = server.ListenAndServeTLS(proxy.HTTPSCert, proxy.HTTPSKey)
	} else {
		log.Printf("http service start, listen on %s", server.Addr)
		err = server.ListenAndServe()
	}
	if err != nil {
		log.Print(err)
		return
	}
}
