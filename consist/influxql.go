// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package consist

import (
    "bufio"
    "bytes"
    "errors"
    "log"
    "strings"
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

func ScanToken(data []byte, atEOF bool) (advance int, token []byte, err error) {
    if atEOF && len(data) == 0 {
        return 0, nil, nil
    }

    start := 0
    for ; start < len(data) && data[start] == ' '; start++ {
        //fmt.Println("start->",start, data[start])
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
