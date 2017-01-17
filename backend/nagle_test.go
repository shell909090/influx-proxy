package backend

import (
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	hb, err := NewHttpBackend("http://localhost:8086", "test")
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
	ca := NewCacheableAPI(hb, 200)

	err = ca.Write([]byte("cpu,host=server01,region=uswest value=1 1434055562000000000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	err = ca.Write([]byte("cpu value=3,value2=4 1434055562000010000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	time.Sleep(500 * time.Millisecond)
}
