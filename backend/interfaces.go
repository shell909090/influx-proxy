package backend

type InfluxAPI interface {
	// Ping() (version string, err error)
	// Query(q string) (err error)
	Write(p []byte) (err error)
}
