package mailru

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/msaf1980/carbon-clickhouse-loader/pkg/driver"
	"github.com/msaf1980/carbon-clickhouse-loader/pkg/tags"
	"github.com/tevino/abool"
	"github.com/vahid-sohrabloo/chconn"
	"github.com/vahid-sohrabloo/chconn/column"
	// "github.com/vahid-sohrabloo/chconn/column"
)

type TaggedDriver struct {
	address string
	table   string

	flushSize uint // metrics max size in bytes

	size    uint                 // size (for flush detect)
	metrics []driver.MetricIndex // metrics buffer

	isRunning *abool.AtomicBool
}

func NewTaggedDriver(address, table string, flushSize uint) (*TaggedDriver, error) {
	if len(address) == 0 {
		address = "clickhouse://127.0.0.1:9000/default"
	}
	return &TaggedDriver{
		address:   address,
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
		conn, err := chconn.Connect(ctx, d.address)
		if err != nil {
			return 0, 0, err
		}

		// fmt.Println("FLUSH")
		dateCols := column.NewDate(false)
		tag1Cols := column.NewString(false)
		pathCols := column.NewString(false)
		versionCols := column.NewUint32(false)

		tagsValues := column.NewString(false)
		tagsCols := column.NewArray(tagsValues)

		for _, m := range d.metrics {
			if path, tags, err := tags.TagsParse(m.Metric); err != nil {
				fmt.Fprintf(os.Stderr, "invalid metric '%s': %v", m.Metric, err)
			} else {
				// fmt.Printf("%s %+v %v\n", name, tags, m.Date)
				for _, tag1 := range tags {
					dateCols.Append(m.Date)
					tag1Cols.AppendString(tag1)
					pathCols.AppendString(path)
					versionCols.Append(0)
					tagsCols.AppendLen(len(tags))
					for _, tag := range tags {
						tagsValues.AppendString(tag)
					}
				}
				n++
			}
		}

		err = conn.Insert(ctx, "INSERT INTO "+d.table+" (Date, Tag1, Path, Tags, Version) VALUES", dateCols, tag1Cols, pathCols, tagsCols, versionCols)
		if err != nil {
			return 0, 0, err
		}

		d.metrics = d.metrics[:0]
		d.size = 0
	}
	return time.Since(start), n, nil
}

func (d *TaggedDriver) Close() error {
	return nil
}
