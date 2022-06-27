package mailru

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/msaf1980/carbon-clickhouse-loader/pkg/driver"
	"github.com/msaf1980/carbon-clickhouse-loader/pkg/tags"
	"github.com/tevino/abool"
)

type TaggedDriver struct {
	address []string
	table   string

	flushSize uint // metrics max size in bytes

	size    uint                 // size (for flush detect)
	metrics []driver.MetricIndex // metrics buffer

	isRunning *abool.AtomicBool
}

func NewTaggedDriver(address, table string, flushSize uint) (*TaggedDriver, error) {
	if len(address) == 0 {
		address = "127.0.0.1:9000"
	}
	return &TaggedDriver{
		address:   []string{address},
		table:     table,
		flushSize: flushSize,
		metrics: make(
			[]driver.MetricIndex,
			0, flushSize/100, // some evristic: size / avg metric length
		),
	}, nil
}

func (d *TaggedDriver) Queued() uint {
	return d.size
}

func (d *TaggedDriver) Write(m driver.MetricIndex) (time.Duration, uint, error) {
	var duration time.Duration
	var n uint
	var err error
	// fmt.Printf("%s %v\n", m.Metric, m.Date)
	if d.size >= d.flushSize {
		if duration, n, err = d.Flush(); err != nil {
			return duration, n, err
		}
	}

	if len(m.Metric) > 0 {
		d.metrics = append(d.metrics, m)
		d.size += uint(len(m.Metric))
	} else {
		return d.Flush()
	}

	return duration, n, nil
}

func (d *TaggedDriver) Flush() (time.Duration, uint, error) {
	var n uint
	start := time.Now()
	if d.size > 0 {
		ctx := context.Background()
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: d.address,
			Auth: clickhouse.Auth{
				Database: "default", // TODO: parse address string and extract name
				Username: "default",
				Password: "",
			},
			//Debug:           true,
			DialTimeout:     time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
		if err != nil {
			return 0, 0, err
		}
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+d.table+" (Date, Tag1, Path, Tags, Version)")
		if err != nil {
			return 0, 0, err
		}

		// fmt.Println("FLUSH")
		for _, m := range d.metrics {
			if path, tags, err := tags.TagsParse(m.Metric); err != nil {
				fmt.Fprintf(os.Stderr, "invalid metric '%s': %v", m.Metric, err)
			} else {
				// fmt.Printf("%s %+v %v\n", name, tags, m.Date)
				for _, tag1 := range tags {
					if err := batch.Append(
						m.Date,
						tag1,
						path,
						tags,
						uint32(0),
					); err != nil {
						return time.Since(start), 0, err
					}
				}
				n++
			}
		}

		if err := batch.Send(); err != nil {
			return time.Since(start), 0, err
		}

		d.metrics = d.metrics[:0]
		d.size = 0
	}
	return time.Since(start), n, nil
}

func (d *TaggedDriver) Close() error {
	return nil
}
