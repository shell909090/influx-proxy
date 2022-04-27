// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestScanKey(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		want string
	}{
		{
			name: "test1",
			line: []byte("cpu1,host=server02 value=0.67,id=2i,running=true,status=\"ok\""),
			want: "cpu1",
		},
		{
			name: "test2",
			line: []byte("cpu2,host=server04,region=us-west,direction=out running=false,status=\"fail\" 1596819659"),
			want: "cpu2",
		},
		{
			name: "test3",
			line: []byte("cpu3,host=server05,region=cn\\ north,tag\\ key=tag\\ value idle=64,system=1i,user=\"Dwayne Johnson\",admin=true"),
			want: "cpu3",
		},
		{
			name: "test4",
			line: []byte("cpu3,host=server06,region=cn\\ south,tag\\ key=value\\=with\"equals\" idle=16,system=16i,user=\"Jay Chou\",admin=false  439888"),
			want: "cpu3",
		},
		{
			name: "test5",
			line: []byte("cpu3,host=server07,region=cn\\ south,tag\\ key=value\\,with\"commas\" idle=74,system=23i,user=\"Stephen Chow\" 440204"),
			want: "cpu3",
		},
		{
			name: "test6",
			line: []byte("cpu4 idle=39,system=56i,user=\"Jay Chou\",brief\\ desc=\"the best \\\"singer\\\"\" 1422568543702"),
			want: "cpu4",
		},
		{
			name: "test7",
			line: []byte("measurement\\ with\\ spaces\\,\\ commas\\ and\\ \"quotes\",tag\\ key\\ with\\ equals\\ \\==tag\\ value\\ with\"spaces\" field_k\\ey\\ with\\ \\==\"string field value, multiple backslashes \\,\\\\,\\\\\\,\\\\\\\\\""),
			want: "measurement with spaces, commas and \"quotes\"",
		},
		{
			name: "test8",
			line: []byte("\"measurement\\ with\\ spaces\\,\\ commas\\ and\\ \"quotes\"\",tag\\ key\\ with\\ equals\\ \\==tag\\,value\\,with\"commas\" field_k\\ey\\ with\\ \\==\"string field value, only \\\" need be escaped\""),
			want: "\"measurement with spaces, commas and \"quotes\"\"",
		},
	}
	for _, tt := range tests {
		got, err := ScanKey(tt.line)
		if err != nil || got != tt.want {
			t.Errorf("%v: got %v, want %v", tt.name, got, tt.want)
			continue
		}
	}
}

func BenchmarkScanKey(b *testing.B) {
	buf := &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(buf, "%s%d,a=%d,b=2 c=3 1596819659\n", "name", i, i)
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

func TestAppendNano(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		time bool
		unit string
		want string
	}{
		{
			name: "test1",
			line: []byte("cpu1 value=1,value2=2"),
			time: false,
			unit: "",
			want: "cpu1 value=1,value2=2",
		},
		{
			name: "test2",
			line: []byte(" cpu1 value=3,value2=4 1422568543702900257 "),
			time: true,
			unit: "",
			want: "cpu1 value=3,value2=4 1422568543702900257",
		},
		{
			name: "test3",
			line: []byte("  cpu2 value=1,value2=2 "),
			time: false,
			unit: "s",
			want: "cpu2 value=1,value2=2",
		},
		{
			name: "test4",
			line: []byte("cpu2 value=3,value2=4 1596819659"),
			time: true,
			unit: "s",
			want: "cpu2 value=3,value2=4 1596819659000000000",
		},
		{
			name: "test5",
			line: []byte("cpu3 value=1,value2=2"),
			time: false,
			unit: "h",
			want: "cpu3 value=1,value2=2",
		},
		{
			name: "test6",
			line: []byte(" cpu3 value=3,value2=4  439888  "),
			time: true,
			unit: "h",
			want: "cpu3 value=3,value2=4  1583596800000000000",
		},
		{
			name: "test7",
			line: []byte("  cpu4 value=1,value2=2  "),
			time: false,
			unit: "ms",
			want: "cpu4 value=1,value2=2",
		},
		{
			name: "test8",
			line: []byte("cpu4 value=3,value2=4  1596819420440"),
			time: true,
			unit: "ms",
			want: "cpu4 value=3,value2=4  1596819420440000000",
		},
		{
			name: "test9",
			line: []byte("\tcpu5 value=3,value2=4  1434055562000010"),
			time: true,
			unit: "u",
			want: "cpu5 value=3,value2=4  1434055562000010000",
		},
		{
			name: "test10",
			line: []byte("\tcpu5 value=3,value2=4 1434055562000010\t"),
			time: true,
			unit: "us",
			want: "cpu5 value=3,value2=4 1434055562000010000",
		},
		{
			name: "test11",
			line: []byte(" \tcpu6 value=1,value2=2 \t"),
			time: false,
			unit: "ns",
			want: "cpu6 value=1,value2=2",
		},
		{
			name: "test12",
			line: []byte(" \tcpu6 value=3,value2=4 1434055562000010000 \t"),
			time: true,
			unit: "ns",
			want: "cpu6 value=3,value2=4 1434055562000010000",
		},
		{
			name: "test13",
			line: []byte("cpu6 value=5,value2=6 1434055562000010000"),
			time: true,
			unit: "n",
			want: "cpu6 value=5,value2=6 1434055562000010000",
		},
	}
	for _, tt := range tests {
		got := AppendNano(tt.line, tt.unit)
		if tt.time {
			if string(got) != tt.want {
				t.Errorf("%v: got %v, want %v", tt.name, string(got), tt.want)
				continue
			}
		} else {
			if strings.Index(string(got), tt.want) != 0 || len(string(got)) != len(tt.want)+20 {
				t.Errorf("%v: got %v, want %v", tt.name, string(got), tt.want)
				continue
			}
		}
	}
}

func BenchmarkAppendNano(b *testing.B) {
	buf := &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(buf, "%s%d,a=%d,b=2 c=3 1596819659\n", "name", i, i)
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
		AppendNano(line, "s")
	}
}

func TestRapidCheck(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		want bool
	}{
		{
			name: "test1",
			line: []byte("cpu1,host=server02,region=us-west value=0.55,id=2i,running=true,status=\"ok\" 1422568543702900257"),
			want: true,
		},
		{
			name: "test2",
			line: []byte("cpu2,host=server04,region=us-west,direction=out running=false,status=\"fail\" 1596819659000000000"),
			want: true,
		},
		{
			name: "test3",
			line: []byte("cpu3,host=server05,region=cn\\ north,tag\\ key=tag\\ value idle=64,system=1i,user=\"Dwayne Johnson\",admin=true 1434055562000010000"),
			want: true,
		},
		{
			name: "test4",
			line: []byte("cpu3,host=server06,region=cn\\ south,tag\\ key=value\\=with\"equals\" idle=16,system=16i,user=\"Jay Chou\",admin=false  1422568543702900257"),
			want: true,
		},
		{
			name: "test5",
			line: []byte("cpu3,host=server07,region=cn\\ south,tag\\ key=value\\,with\"commas\" idle=74,system=23i,user=\"Stephen Chow\" 1583599143422568543"),
			want: true,
		},
		{
			name: "test6",
			line: []byte("cpu4 idle=39,system=56i,user=\"Jay Chou\",brief\\ desc=\"the best \\\"singer\\\"\" 1422568543702000000"),
			want: true,
		},
		{
			name: "test7",
			line: []byte("cpu4 idle=47,system=93i,user=\"Stephen Chow\",admin=true,brief\\ desc=\"the best \\\"novelist\\\"\"  1596819420440000000"),
			want: true,
		},
		{
			name: "test8",
			line: []byte("measurement\\ with\\ spaces\\,\\ commas\\ and\\ \"quotes\",tag\\ key\\ with\\ equals\\ \\==tag\\ value\\ with\"spaces\" field_k\\ey\\ with\\ \\==\"string field value, multiple backslashes \\,\\\\,\\\\\\,\\\\\\\\\""),
			want: true,
		},
		{
			name: "test9",
			line: []byte("\"measurement\\ with\\ spaces\\,\\ commas\\ and\\ \"quotes\"\",tag\\ key\\ with\\ equals\\ \\==tag\\,value\\,with\"commas\" field_k\\ey\\ with\\ \\==\"string field value, only \\\" need be escaped\""),
			want: true,
		},
		{
			name: "test10",
			line: []byte("cpu  1422568543702900257"),
			want: false,
		},
		{
			name: "test11",
			line: []byte("cpu\\ idle  1422568543702900257"),
			want: false,
		},
		{
			name: "test12",
			line: []byte("cpu\\ idle,host=server02,region=us-west 1422568543702900257"),
			want: false,
		},
		{
			name: "test13",
			line: []byte("cpu,host=server02,region\\ area=us-west 1422568543702900257"),
			want: false,
		},
		{
			name: "test14",
			line: []byte("cpu,host=server02,region=us\\ west 1422568543702900257"),
			want: false,
		},
		{
			name: "test15",
			line: []byte("cpu,host=server02,region=us-west   1422568543702900257"),
			want: false,
		},
		{
			name: "test16",
			line: []byte("cpu,host=server02,region=us-west brief\\ value=0.93 1422568543702900257"),
			want: true,
		},
		{
			name: "test17",
			line: []byte("cpu,host=server02,region=us\\ west brief\\ value=0.93 1422568543702900257"),
			want: true,
		},
		{
			name: "test18",
			line: []byte("cpu,host=server02,region=us-west brief\\ desc=\"test server\" 1422568543702900257"),
			want: true,
		},
	}
	for _, tt := range tests {
		got := RapidCheck(tt.line)
		if got != tt.want {
			t.Errorf("%v: got %v, want %v", tt.name, got, tt.want)
			continue
		}
	}
}

func BenchmarkRapidCheck(b *testing.B) {
	buf := &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(buf, "%s%d,a=%d,b=2 c=3 1596819659\n", "name", i, i)
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
		RapidCheck(line)
	}
}
