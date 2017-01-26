package backend

import (
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	cfg, ts := CreateTestBackendConfig("test")
	defer ts.Close()
	bs, err := NewBackends(cfg, "test")
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
	defer bs.Close()

	err = bs.Write([]byte("cpu,host=server01,region=uswest value=1 1434055562000000000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	err = bs.Write([]byte("cpu value=3,value2=4 1434055562000010000"))
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	time.Sleep(300 * time.Millisecond)
	// FIXME: just once?
}
