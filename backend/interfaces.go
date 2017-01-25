package backend

import "net/http"

type Pinger interface {
	IsActive() (b bool)
	Ping() (version string, err error)
}

type Querist interface {
	Query(w http.ResponseWriter, req *http.Request) (err error)
}

type WriteCloser interface {
	Write(p []byte) (err error)
	Close() (err error)
}

type InfluxAPI interface {
	Pinger
	Querist
	WriteCloser
}
