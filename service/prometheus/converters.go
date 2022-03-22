package prometheus

import (
	"fmt"
	"math"
	"time"

	"github.com/chengshiwen/influx-proxy/service/prometheus/remote"
	"github.com/influxdata/influxdb1-client/models"
)

const (
	// measurementName is the default name used if no Prometheus name can be found on write
	measurementName = "prom_metric_not_specified"

	// fieldName is the field all prometheus values get written to
	fieldName = "value"

	// prometheusNameTag is the tag key that Prometheus uses for metric names
	prometheusNameTag = "__name__"
)

// A DroppedValuesError is returned when the prometheus write request contains
// unsupported float64 values.
type DroppedValuesError struct {
	nan  uint64
	ninf uint64
	inf  uint64
}

// Error returns a descriptive error of the values dropped.
func (e DroppedValuesError) Error() string {
	return fmt.Sprintf("dropped unsupported Prometheus values: [NaN = %d, +Inf = %d, -Inf = %d]", e.nan, e.inf, e.ninf)
}

// WriteRequestToPoints converts a Prometheus remote write request of time series and their
// samples into Points that can be written into Influx
func WriteRequestToPoints(req *remote.WriteRequest) ([]models.Point, error) {
	var maxPoints int
	for _, ts := range req.Timeseries {
		maxPoints += len(ts.Samples)
	}
	points := make([]models.Point, 0, maxPoints)

	// Track any dropped values.
	var nan, inf, ninf uint64

	for _, ts := range req.Timeseries {
		measurement := measurementName

		tags := make(map[string]string, len(ts.Labels))
		for _, l := range ts.Labels {
			tags[l.Name] = l.Value
			if l.Name == prometheusNameTag {
				measurement = l.Value
			}
		}

		for _, s := range ts.Samples {
			if v := s.Value; math.IsNaN(v) {
				nan++
				continue
			} else if math.IsInf(v, -1) {
				ninf++
				continue
			} else if math.IsInf(v, 1) {
				inf++
				continue
			}

			// convert and append
			t := time.Unix(0, s.TimestampMs*int64(time.Millisecond))
			fields := map[string]interface{}{fieldName: s.Value}
			p, err := models.NewPoint(measurement, models.NewTags(tags), fields, t)
			if err != nil {
				return nil, err
			}
			points = append(points, p)
		}
	}

	if nan+inf+ninf > 0 {
		return points, DroppedValuesError{nan: nan, inf: inf, ninf: ninf}
	}
	return points, nil
}
