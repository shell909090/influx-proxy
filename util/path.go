package util

import (
    "os"
)

// PathExists 检查目录是否存在
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
