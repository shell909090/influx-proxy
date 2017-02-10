package main

import (
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"

	redis "gopkg.in/redis.v5"

	"github.com/shell909090/influx-proxy/backend"
)

var (
	ErrConfig  = errors.New("config parse error")
	ConfigFile string
	NodeName   string
	RedisAddr  string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&ConfigFile, "config", "", "config file")
	flag.StringVar(&NodeName, "node", "l1", "node name")
	flag.StringVar(&RedisAddr, "redis", "localhost:6379", "config file")
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

func main() {
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

	ic := backend.NewInfluxCluster(rcs, nodecfg.Zone)
	ic.LoadConfig()

	mux := http.NewServeMux()
	NewHttpService(ic, nodecfg.DB).Register(mux)

	log.Printf("http service start.")
	err = http.ListenAndServe(nodecfg.ListenAddr, mux)
	if err != nil {
		log.Print(err)
		return
	}
}
