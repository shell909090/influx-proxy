// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"net/url"
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

	params := &url.Values{}
	params.Add("rp", "two_hours")

	err = bs.Write(
		&Record{
			Params: params,
			Body:   []byte("cpu,host=server01,region=uswest value=1 1434055562000000000"),
		})
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	err = bs.Write(
		&Record{
			Params: params,
			Body:   []byte("cpu value=3,value2=4 1434055562000010000"),
		})
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}

	time.Sleep(time.Second)
	// FIXME: just once?
}

func TestRewrite(t *testing.T) {
	cfg, ts := CreateTestBackendConfig("test")
	defer ts.Close()
	bs, err := NewBackends(cfg, "test")
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
	defer bs.Close()
	for i := 0; i < 100; i++ {
		err := bs.fb.Write([]byte("cpu value=3,value2=4 1434055562000010000"))
		if err != nil {
			t.Errorf("error: %s", err)
			return
		}
	}
	time.Sleep(2 * time.Second)
}
