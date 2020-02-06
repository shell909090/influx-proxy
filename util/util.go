package util

import "os"

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
