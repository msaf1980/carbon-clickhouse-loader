package driver

import (
	"fmt"
	"time"
)

var ErrMetricNotSupported = fmt.Errorf("metric not supported")

type MetricIndex struct {
	Metric string
	Date   time.Time
}

type Driver interface {
	Write(MetricIndex) (time.Duration, uint, error)
	Flush() (time.Duration, uint, error)
	Close() error
	Queued() uint
}
