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
	"github.com/shell909090/influx-proxy/service"
)

var (
	ErrConfig  = errors.New("config parse error")
	ConfigFile string
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.StringVar(&ConfigFile, "config", "/etc/influxdb/proxy.json", "config file")
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

type NodeConfig struct {
	ListenAddr string
	DB         string
	Zone       string
}

var (
	cfg     Config
	nodecfg NodeConfig
)

func LoadConfig() (client *redis.Client, err error) {
	err = LoadJson(ConfigFile, &cfg)
	if err != nil {
		log.Print("load config failed: ", err)
		return
	}
	log.Printf("json loaded.")

	client = redis.NewClient(&cfg.Options)
	val, err := client.HGetAll("n:" + cfg.Node).Result()
	if err != nil {
		log.Printf("redis load error: b:%s", cfg.Node)
		return
	}

	err = backend.LoadStructFromMap(val, &nodecfg)
	if err != nil {
		log.Printf("redis load error: b:%s", cfg.Node)
		return
	}
	log.Printf("node config loaded.")
	return
}

func main() {
	client, err := LoadConfig()
	if err != nil {
		return
	}

	m := backend.NewMultiAPI(client, nodecfg.Zone)
	m.LoadConfig()
	hs := service.NewHttpService(m, nodecfg.DB)

	mux := http.NewServeMux()
	hs.Register(mux)

	log.Printf("http service start.")
	err = http.ListenAndServe(nodecfg.ListenAddr, mux)
	if err != nil {
		log.Print(err)
		return
	}
}
