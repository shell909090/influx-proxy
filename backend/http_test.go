package backend

import "testing"

func TestHttpBackend(t *testing.T) {
	hb, err := NewHttpBackend("http://localhost:8086", "test")
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	err = hb.Write([]byte("cpu,host=server01,region=uswest value=1 1434055562000000000\ncpu value=3,value2=4 1434055562000010000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
}
