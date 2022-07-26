// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package util

import (
	"encoding/json"
	"fmt"
	"os"

	jsoniter "github.com/json-iterator/go"
)

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

func MarshalJSON(v interface{}, pretty bool) []byte {
	var res []byte
	if pretty {
		res, _ = json.MarshalIndent(v, "", "    ")
	} else {
		res, _ = json.Marshal(v)
	}
	res = append(res, '\n')
	return res
}

func CastString(v interface{}) string {
	switch tv := v.(type) {
	case json.Number:
		return tv.String()
	case jsoniter.Number:
		return tv.String()
	case string:
		return tv
	}
	return fmt.Sprint(v)
}
