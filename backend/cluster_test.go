// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func TestScanKey(t *testing.T) {
	var points []string = []string{
		"cpu,host=server01,region=uswest value=1 1434055562000000000",
		"cpu value=3,value2=4 1434055562000010000",
		"temper\\ ature,machine=unit42,type=assembly internal=32,external=100 1434055562000000035",
		"temper\\,ature,machine=unit143,type=assembly internal=22,external=130 1434055562005000035",
	}
	var keys []string = []string{
		"cpu",
		"cpu",
		"temper ature",
		"temper,ature",
	}

	var key string
	var err error
	for i, s := range points {
		key, err = ScanKey([]byte(s))
		if err != nil {
			t.Errorf("error: %s", err)
			return
		}
		if key != keys[i] {
			t.Errorf("quota test failed: %s, %s", key, keys[i])
			return
		}
	}

	return
}

func BenchmarkScanKey(b *testing.B) {
	buf := &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(buf, "%s%d,a=%d,b=2 c=3 10000\n", "name", i, i)
	}
	b.ResetTimer()

	var err error
	var line []byte
	for {
		line, err = buf.ReadBytes('\n')
		switch err {
		default:
			b.Error(err)
			return
		case io.EOF, nil:
		}

		if len(line) == 0 {
			break
		}

		line = bytes.TrimRight(line, " \t\r\n")
		_, err = ScanKey(line)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
