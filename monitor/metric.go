package monitor

import (
	"fmt"
	"time"
)

// type Metric struct {
// 	Name   string                 `json:"name"`
// 	Tags   map[string]string      `json:"tags"`
// 	Fields map[string]interface{} `json:"fields"`
// 	Time   time.Time              `json:"time"`
// }

// func (m *Metric) ParseToLine() (line string, err error) {
// 	p, err := models.NewPoint(m.Name, m.Tags, m.Fields, m.Time)
// 	if err != nil {
// 		return "", err
// 	}
// 	line = p.PrecisionString("ns")

// 	return
// }

func DataToLine(name string, tags map[string]string, fields map[string]interface{}, time time.Time) (line string, err error) {
	line = name
	for key, value := range tags {
		line += fmt.Sprintf(",%s=%s", key, value)
	}
	line += " "
	for key, value := range fields {
		switch value := value.(type) {
		case int64:
			line += fmt.Sprintf("%s=%d,", key, value)
		}
	}
	line = line[:len(line)-1] + fmt.Sprintf(" %d", time.UnixNano())

	return
}
