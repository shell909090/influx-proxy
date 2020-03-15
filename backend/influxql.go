// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
    "bufio"
    "bytes"
    "errors"
    "github.com/influxdata/influxdb1-client/models"
    "io"
    "log"
    "strconv"
    "strings"
    "time"
)

var (
    ErrWrongQuote     = errors.New("wrong quote")
    ErrUnmatchedQuote = errors.New("unmatched quote")
    ErrUnclosed       = errors.New("unclosed parenthesis")
    ErrIllegalQL      = errors.New("illegal InfluxQL")
)

func FindEndWithQuote(data []byte, start int, endchar byte) (end int, unquoted []byte, err error) {
    unquoted = append(unquoted, data[start])
    start++
    for end = start; end < len(data); end++ {
        switch data[end] {
        case endchar:
            unquoted = append(unquoted, data[end])
            end++
            return
        case '\\':
            switch {
            case len(data) == end:
                err = ErrUnmatchedQuote
                return
            case data[end+1] == endchar:
                end++
                unquoted = append(unquoted, data[end])
            default:
                err = ErrWrongQuote
                return
            }
        default:
            unquoted = append(unquoted, data[end])
        }
    }
    err = ErrUnmatchedQuote
    return
}

func ScanKey(pointbuf []byte) (key string, err error) {
    keyslice := make([]byte, 0)
    buflen := len(pointbuf)
    for i := 0; i < buflen; i++ {
        c := pointbuf[i]
        switch c {
        case '\\':
            i++
            keyslice = append(keyslice, pointbuf[i])
        case ' ', ',':
            key = string(keyslice)
            return
        default:
            keyslice = append(keyslice, c)
        }
    }
    return "", io.EOF
}

func ScanToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }

    start := 0
    for ; start < len(data) && data[start] == ' '; start++ {
        // fmt.Println("start->",start, data[start])
    }

    if start == len(data) {
        return 0, nil, nil
    }

    switch data[start] {
    case '"':
        advance, token, err = FindEndWithQuote(data, start, '"')
        if err != nil {
            log.Printf("scan token error: %s\n", err)
        }
        return
    case '\'':
        advance, token, err = FindEndWithQuote(data, start, '\'')
        if err != nil {
            log.Printf("scan token error: %s\n", err)
        }
        return
    case '(':
        advance = bytes.IndexByte(data[start:], ')')
        if advance == -1 {
            err = ErrUnclosed
        } else {
            advance += start + 1
        }
    case '[':
        advance = bytes.IndexByte(data[start:], ']')
        if advance == -1 {
            err = ErrUnclosed
        } else {
            advance += start + 1
        }
    case '{':
        advance = bytes.IndexByte(data[start:], '}')
        if advance == -1 {
            err = ErrUnclosed
        } else {
            advance += start + 1
        }
    default:
        advance = bytes.IndexFunc(data[start:], func(r rune) bool {
            return r == ' '
        })
        if advance == -1 {
            advance = len(data)
        } else {
            advance += start
        }

    }
    if err != nil {
        log.Printf("scan token error: %s\n", err)
        return
    }

    token = data[start:advance]
    return
}

func GetMeasurementFromInfluxQL(q string) (m string, err error) {
    buf := bytes.NewBuffer([]byte(q))
    scanner := bufio.NewScanner(buf)
    scanner.Buffer([]byte(q), len(q))
    scanner.Split(ScanToken)
    var tokens []string
    for scanner.Scan() {
        tokens = append(tokens, scanner.Text())
    }

    for i := 0; i < len(tokens); i++ {
        if strings.ToLower(tokens[i]) == "from" || strings.ToLower(tokens[i]) == "measurement" {
            if i+1 < len(tokens) {
                m = getMeasurement(tokens[i+1:])
                return
            }
        }
    }

    return "", ErrIllegalQL
}

func getMeasurement(tokens []string) (m string) {
    if len(tokens) >= 2 && strings.HasPrefix(tokens[1], ".") {
        m = tokens[1]
        m = m[1:]
        if m[0] == '"' || m[0] == '\'' {
            m = m[1: len(m)-1]
        }
        return
    }

    m = tokens[0]
    if m[0] == '/' {
        return m
    }

    if m[0] == '"' || m[0] == '\'' {
        m = m[1: len(m)-1]
        return
    }

    index := strings.IndexByte(m, '.')
    if index == -1 {
        return
    }

    m = m[index+1:]
    if m[0] == '"' || m[0] == '\'' {
        m = m[1: len(m)-1]
    }
    return
}

func Int64ToBytes(n int64) []byte {
    return []byte(strconv.FormatInt(n, 10))
}

func BytesToInt64(buf []byte) int64 {
    var res int64 = 0
    var length = len(buf)
    for i := 0; i < length; i++ {
        res = res * 10 + int64(buf[i]-'0')
    }
    return res
}

func ScanTime(buf []byte) (int, bool) {
    i := len(buf) - 1
    for ; i >= 0; i-- {
        if buf[i] < '0' || buf[i] > '9' {
            break
        }
    }
    return i, i > 0 && i < len(buf) - 1 && (buf[i] == ' ' || buf[i] == '\t' || buf[i] == 0)
}

func LineToNano(line []byte, precision string) []byte {
    line = bytes.TrimRight(line, " \t\r\n")
    if precision != "ns" {
        if pos, found := ScanTime(line); found {
            if precision == "u" {
                return append(line, []byte("000")...)
            } else if precision == "ms" {
                return append(line, []byte("000000")...)
            } else if precision == "s" {
                return append(line, []byte("000000000")...)
            } else {
                mul := models.GetPrecisionMultiplier(precision)
                nano := BytesToInt64(line[pos+1:]) * mul
                bytenano := Int64ToBytes(nano)
                return bytes.Join([][]byte{line[:pos], bytenano}, []byte(" "))
            }
        }
    } else {
        if _, found := ScanTime(line); !found {
            return append(line, []byte(" " + strconv.FormatInt(time.Now().UnixNano(), 10))...)
        }
    }
    return line
}
