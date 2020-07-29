package util

import (
	"bytes"
	"encoding/json"
	"github.com/deckarep/golang-set"
	gzip "github.com/klauspost/pgzip"
	"io"
	"os"
)

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func NewSetFromStrSlice(s []string) mapset.Set {
	set := mapset.NewSet()
	for _, v := range s {
		set.Add(v)
	}
	return set
}

func PathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func MakeDir(dir string) (err error) {
	exist, err := PathExist(dir)
	if err != nil {
		return
	}
	if !exist {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return
		}
	}
	return
}

func GzipCompress(b []byte) (cb []byte, err error) {
	var buf bytes.Buffer
	zip := gzip.NewWriter(&buf)
	n, err := zip.Write(b)
	if err != nil {
		return
	}
	if n != len(b) {
		err = io.ErrShortWrite
		return
	}
	err = zip.Close()
	cb = buf.Bytes()
	return
}

func MarshalJson(v interface{}, pretty bool, newline bool) []byte {
	var res []byte
	if pretty {
		res, _ = json.MarshalIndent(v, "", "    ")
	} else {
		res, _ = json.Marshal(v)
	}
	if newline {
		res = append(res, '\n')
	}
	return res
}
