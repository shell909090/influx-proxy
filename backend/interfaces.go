package backend

import "net/http"

type InfluxAPI interface {
	Ping() (version string, err error)
	Query(w http.ResponseWriter, req *http.Request) (err error)
	Write(p []byte) (err error)
}
