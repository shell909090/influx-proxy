package util

import (
    "bytes"
    "compress/gzip"
    "io"
    "os"
)

func MapHasKey(m map[string]bool, k string) bool {
    if _, ok := m[k]; ok {
        return true
    }
    return false
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
