package util

import (
    "bytes"
    "compress/gzip"
    "io"
    "net/http"
    "os"
)

func ContainString(arr []string, t string) bool {
    for _, v := range arr {
        if v == t {
            return true
        }
    }
    return false
}

func ContainInt(arr []int, t int) bool {
    for _, v := range arr {
        if v == t {
            return true
        }
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

func StatusText(status int) []byte {
    return []byte(http.StatusText(status) + "\n")
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
