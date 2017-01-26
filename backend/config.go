package backend

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/redis.v5"
)

const (
	VERSION = "1.1"
)

var (
	ErrIllegalConfig = errors.New("illegal config")
)

func LoadStructFromMap(data map[string]string, o interface{}) (err error) {
	var x int
	val := reflect.ValueOf(o).Elem()
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		typeField := val.Type().Field(i)

		name := strings.ToLower(typeField.Name)
		s, ok := data[name]
		if !ok {
			continue
		}

		switch typeField.Type.Kind() {
		case reflect.String:
			valueField.SetString(s)
		case reflect.Int:
			x, err = strconv.Atoi(s)
			if err != nil {
				log.Printf("%s: %s", err, name)
				return
			}
			valueField.SetInt(int64(x))
		}
	}
	return
}

type BackendConfig struct {
	URL          string
	DB           string
	Zone         string
	Interval     int
	Timeout      int
	TimeoutQuery int
}

func LoadConfigFromRedis(client *redis.Client, name string) (cfg *BackendConfig, err error) {
	val, err := client.HGetAll("b:" + name).Result()
	if err != nil {
		log.Printf("redis load error: b:%s", name)
		return
	}

	cfg = &BackendConfig{}
	err = LoadStructFromMap(val, cfg)
	if err != nil {
		return
	}

	if cfg.Interval == 0 {
		cfg.Interval = 200
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 4000
	}
	if cfg.TimeoutQuery == 0 {
		cfg.TimeoutQuery = 60000
	}
	return
}
