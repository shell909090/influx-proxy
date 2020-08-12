package util

import "strings"

var (
	identifierEscaper    = strings.NewReplacer(`"`, `\"`)
	identifierUnescaper  = strings.NewReplacer(`\"`, `"`)
	measurementEscaper   = strings.NewReplacer(`,`, `\,`, ` `, `\ `)
	measurementUnescaper = strings.NewReplacer(`\,`, `,`, `\ `, ` `)
	tagEscaper           = strings.NewReplacer(`,`, `\,`, ` `, `\ `, `=`, `\=`)
	tagUnescaper         = strings.NewReplacer(`\,`, `,`, `\ `, ` `, `\=`, `=`)
)

func EscapeIdentifier(in string) string {
	return identifierEscaper.Replace(in)
}

func UnescapeIdentifier(in string) string {
	if strings.IndexByte(in, '\\') == -1 {
		return in
	}
	return identifierUnescaper.Replace(in)
}

func EscapeMeasurement(in string) string {
	return measurementEscaper.Replace(in)
}

func UnescapeMeasurement(in string) string {
	if strings.IndexByte(in, '\\') == -1 {
		return in
	}
	return measurementUnescaper.Replace(in)
}

func EscapeTag(in string) string {
	return tagEscaper.Replace(in)
}

func UnescapeTag(in string) string {
	if strings.IndexByte(in, '\\') == -1 {
		return in
	}
	return tagUnescaper.Replace(in)
}
