package backend

const (
	VERSION = "1.1"
)

type BackendConfig struct {
	URL          string
	DB           string
	Interval     int
	Timeout      int
	TimeoutQuery int
}

func (cfg *BackendConfig) CreateCacheableHttp() (ca *CacheableAPI) {
	hb := NewHttpBackend(cfg)
	ca = NewCacheableAPI(hb, cfg)
	return
}
