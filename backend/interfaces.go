package backend

import "net/http"

type Pinger interface {
	Ping() (version string, err error)
}

type Querist interface {
	Query(w http.ResponseWriter, req *http.Request) (err error)
}

type Writer interface {
	Write(p []byte) (err error)
}

type InfluxAPI interface {
	Pinger
	Querist
	Writer
}
