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

func PathExists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil {
        return true, nil
    }
    if os.IsNotExist(err) {
        return false, nil
    }
    return false, err
}

func CheckPathAndCreate(StoreDir string) {
    exist, err := PathExists(StoreDir)
    if err != nil {
        return
    }
    if !exist {
        err = os.MkdirAll(StoreDir, os.ModePerm)
        if err != nil {
            return
        }
    }
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
