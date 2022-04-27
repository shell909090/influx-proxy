// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/models"
)

type LinePoint struct {
	Db   string
	Rp   string
	Line []byte
}

func ScanKey(pointbuf []byte) (key string, err error) {
	buflen := len(pointbuf)
	var b strings.Builder
	b.Grow(buflen)
	for i := 0; i < buflen; i++ {
		c := pointbuf[i]
		switch c {
		case '\\':
			i++
			b.WriteByte(pointbuf[i])
		case ' ', ',':
			key = b.String()
			return
		default:
			b.WriteByte(c)
		}
	}
	return "", io.EOF
}

func ScanTime(buf []byte) (int, bool) {
	i := len(buf) - 1
	for ; i >= 0; i-- {
		if buf[i] < '0' || buf[i] > '9' {
			break
		}
	}
	return i, i > 0 && i < len(buf)-1 && (buf[i] == ' ' || buf[i] == 0)
}

func AppendNano(line []byte, precision string) []byte {
	line = bytes.TrimSpace(line)
	pos, found := ScanTime(line)
	if found {
		if precision == "ns" || precision == "n" {
			return line
		} else if precision == "us" || precision == "u" {
			return append(line, '0', '0', '0')
		} else if precision == "ms" {
			return append(line, '0', '0', '0', '0', '0', '0')
		} else if precision == "s" {
			return append(line, '0', '0', '0', '0', '0', '0', '0', '0', '0')
		} else {
			mul := models.GetPrecisionMultiplier(precision)
			nano := BytesToInt64(line[pos+1:]) * mul
			return append(line[:pos+1], Int64ToBytes(nano)...)
		}
	} else {
		return append(line, []byte(" "+strconv.FormatInt(time.Now().UnixNano(), 10))...)
	}
}

func Int64ToBytes(n int64) []byte {
	return []byte(strconv.FormatInt(n, 10))
}

func BytesToInt64(buf []byte) int64 {
	var res int64 = 0
	var length = len(buf)
	for i := 0; i < length; i++ {
		res = res*10 + int64(buf[i]-'0')
	}
	return res
}

func RapidCheck(buf []byte) bool {
	buflen := len(buf)
	// find the first unescaped space, and pick the last for consecutive spaces
	i := 0
Loop:
	for i < buflen {
		switch buf[i] {
		case '\\':
			i += 2
		case ' ':
			for i < buflen-1 {
				if buf[i+1] == ' ' {
					i++
				} else {
					break Loop
				}
			}
		default:
			i++
		}
	}
	// find the last unescaped space, and pick the first for consecutive spaces
	// buf has trimmed right spaces and ends with timestamp
	j := bytes.LastIndexByte(buf, ' ')
	for j > 0 {
		if buf[j-1] == ' ' {
			j--
		} else {
			break
		}
	}
	return j-i > 3
}
