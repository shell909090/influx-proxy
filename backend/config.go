package backend

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
