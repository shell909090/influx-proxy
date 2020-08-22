package backend

import (
	"bufio"
	"bytes"
	"errors"
	"log"
	"strings"

	"github.com/chengshiwen/influx-proxy/util"
)

var SupportCmds = util.NewSet(
	"show measurements",
	"show series",
	"show field keys",
	"show tag keys",
	"show tag values",
	"show retention policies",
	"show stats",
	"show databases",
	"create database",
	"drop database",
	"delete from",
	"drop series from",
	"drop measurement",
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
	}
	if start == len(data) {
		return 0, nil, nil
	}

	switch data[start] {
	case '"':
		advance, token, err = FindEndWithQuote(data, start, '"')
		if err != nil {
			log.Printf("scan token error: %s", err)
		}
		return
	case '\'':
		advance, token, err = FindEndWithQuote(data, start, '\'')
		if err != nil {
			log.Printf("scan token error: %s", err)
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
	case '.':
		advance = start + 1
		for ; advance < len(data); advance++ {
			if data[advance] != '.' {
				break
			}
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
		log.Printf("scan token error: %s", err)
		return
	}

	token = data[start:advance]
	// fmt.Printf("%s (%d, %d) = %s\n", data, start, advance, token)
	return
}

func ScanTokens(q string, n int) (tokens []string) {
	q = strings.TrimRight(strings.TrimSpace(q), "; ")
	buf := bytes.NewBuffer([]byte(q))
	scanner := bufio.NewScanner(buf)
	scanner.Buffer([]byte(q), len(q))
	scanner.Split(ScanToken)
	for scanner.Scan() {
		tokens = append(tokens, scanner.Text())
		if n > 0 && len(tokens) == n {
			return
		}
	}
	return
}

func GetHeadStmtFromTokens(tokens []string, n int) (stmt string) {
	if n <= 0 || n > len(tokens) {
		n = len(tokens)
	}
	return strings.ToLower(strings.Join(tokens[:n], " "))
}

func GetDatabaseFromInfluxQL(q string) (m string, err error) {
	return GetDatabaseFromTokens(ScanTokens(q, 0))
}

func GetMeasurementFromInfluxQL(q string) (m string, err error) {
	return GetMeasurementFromTokens(ScanTokens(q, 0))
}

func GetDatabaseFromTokens(tokens []string) (m string, err error) {
	return GetIdentifierFromTokens(tokens, []string{"on", "database"}, getDatabase)
}

func GetMeasurementFromTokens(tokens []string) (m string, err error) {
	return GetIdentifierFromTokens(tokens, []string{"from", "measurement"}, getMeasurement)
}

func GetIdentifierFromTokens(tokens []string, keywords []string, fn func([]string) string) (m string, err error) {
	for i := 0; i < len(tokens); i++ {
		for j := 0; j < len(keywords); j++ {
			if strings.ToLower(tokens[i]) == keywords[j] {
				if i+1 < len(tokens) {
					m = fn(tokens[i+1:])
					return
				}
			}
		}
	}
	return "", ErrIllegalQL
}

func getDatabase(tokens []string) (m string) {
	m = tokens[0]
	if m[0] == '"' || m[0] == '\'' {
		m = m[1 : len(m)-1]
		return
	}

	index := strings.IndexByte(m, '.')
	if index == -1 {
		return
	}

	m = m[:index]
	return
}

func getMeasurement(tokens []string) (m string) {
	if len(tokens) >= 3 && (tokens[1] == "." || tokens[1] == "..") {
		if len(tokens) > 3 && tokens[1] == "." {
			if len(tokens) >= 5 && tokens[3] == "." {
				m = tokens[4]
			} else {
				m = tokens[0]
			}
		} else {
			m = tokens[2]
		}
		if m[0] == '"' || m[0] == '\'' {
			m = m[1 : len(m)-1]
		}
		return
	}

	m = tokens[0]
	if m[0] == '/' {
		return m
	}

	if m[0] == '"' || m[0] == '\'' {
		m = m[1 : len(m)-1]
		return
	}

	index := strings.IndexByte(m, '.')
	if index == -1 {
		return
	}

	m = m[index+1:]
	if m[0] == '"' || m[0] == '\'' {
		m = m[1 : len(m)-1]
	}
	return
}

func CheckQuery(q string) (tokens []string, check bool, from bool) {
	tokens = ScanTokens(q, 0)
	stmt := strings.ToLower(tokens[0])
	if stmt == "select" {
		for i := 2; i < len(tokens); i++ {
			stmt := strings.ToLower(tokens[i])
			if stmt == "into" {
				return tokens, false, false
			}
			if stmt == "from" {
				return tokens, true, true
			}
		}
		return tokens, false, false
	}
	if stmt == "show" {
		for i := 2; i < len(tokens); i++ {
			stmt := strings.ToLower(tokens[i])
			if stmt == "from" {
				check = SupportCmds[GetHeadStmtFromTokens(tokens, i)] || SupportCmds[GetHeadStmtFromTokens(tokens, i-2)]
				return tokens, check, true
			}
		}
	}
	stmt2 := GetHeadStmtFromTokens(tokens, 2)
	if SupportCmds[stmt2] {
		return tokens, true, stmt2 == "delete from" || stmt2 == "drop measurement"
	}
	stmt3 := GetHeadStmtFromTokens(tokens, 3)
	if SupportCmds[stmt3] {
		return tokens, true, stmt3 == "drop series from"
	}
	return tokens, false, false
}

func CheckDatabaseFromTokens(tokens []string) (check bool, show bool, alter bool, db string) {
	stmt := GetHeadStmtFromTokens(tokens, 2)
	show = stmt == "show databases"
	alter = stmt == "create database" || stmt == "drop database"
	check = show || alter
	if alter && len(tokens) >= 3 {
		db = getDatabase(tokens[2:])
	}
	return
}

func CheckSelectOrShowFromTokens(tokens []string) (check bool) {
	stmt := strings.ToLower(tokens[0])
	check = stmt == "select" || stmt == "show"
	return
}

func CheckDeleteOrDropMeasurementFromTokens(tokens []string) (check bool) {
	if len(tokens) >= 3 {
		stmt := GetHeadStmtFromTokens(tokens, 2)
		return stmt == "delete from" || stmt == "drop measurement" || stmt == "drop series"
	}
	return
}
