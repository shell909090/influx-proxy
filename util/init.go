package util

func IncludeString(arr []string, t string) bool {
    for _, v := range arr {
        if v == t {
            return true
        }
    }
    return false
}

func IncludeInt(arr []int, t int) bool {
    for _, v := range arr {
        if v == t {
            return true
        }
    }
    return false
}
