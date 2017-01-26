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

	ic := backend.NewInfluxCluster(client, nodecfg.Zone)
	ic.LoadConfig()
	hs := NewHttpService(ic, nodecfg.DB)

	mux := http.NewServeMux()
	hs.Register(mux)

	log.Printf("http service start.")
	err = http.ListenAndServe(nodecfg.ListenAddr, mux)
	if err != nil {
		log.Print(err)
		return
	}
}
