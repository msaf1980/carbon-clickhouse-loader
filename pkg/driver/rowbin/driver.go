package mailru

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"github.com/msaf1980/carbon-clickhouse-loader/pkg/RowBinary"
	"github.com/msaf1980/carbon-clickhouse-loader/pkg/driver"
	"github.com/msaf1980/carbon-clickhouse-loader/pkg/tags"
	"github.com/tevino/abool"
)

type TaggedDriver struct {
	query string

	flushSize uint // metrics max size in bytes

	size    uint                 // size (for flush detect)
	metrics []driver.MetricIndex // metrics buffer

	isRunning *abool.AtomicBool
}

func NewTaggedDriver(address, table string, flushSize uint) *TaggedDriver {
	if len(address) == 0 {
		address = "http://127.0.0.1:8123"
	}

	p, err := url.Parse(address)
	if err != nil {
		return nil
	}
	q := p.Query()

	q.Set("query", "INSERT INTO "+table+" (Date, Tag1, Path, Tags, Version) FORMAT RowBinary")
	p.RawQuery = q.Encode()

	return &TaggedDriver{
		query:     p.String(),
		flushSize: flushSize,
		metrics: make(
			[]driver.MetricIndex,
			0, flushSize/100, // some evristic: size / avg metric length
		),
	}
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
	var sended uint32
	start := time.Now()
	if d.size > 0 {
		pr, pw := io.Pipe()

		go func() {
			defer pw.Close()

			var tagsBuf bytes.Buffer
			var buf bytes.Buffer
			tagsBuf.Grow(4096)
			buf.Grow(512 * 1024)
			var n uint32
			for _, m := range d.metrics {
				if path, tags, err := tags.TagsParse(m.Metric); err != nil {
					fmt.Fprintf(os.Stderr, "invalid metric '%s': %v", m.Metric, err)
				} else {
					// fmt.Printf("%s %+v %v\n", name, tags, m.Date)
					tagsBuf.Reset()
					buf.Reset()
					RowBinary.NewWriter(&tagsBuf).WriteStringList(tags)
					w := RowBinary.NewWriter(&buf)
					for _, tag1 := range tags {
						// Date, Tag1, Path, Tags, Version
						w.WriteDate(m.Date)
						w.WriteString(tag1)
						w.WriteString(path)
						w.Write(tagsBuf.Bytes())
						w.WriteUint32(uint32(start.Unix()))
					}
					if _, err := pw.Write(buf.Bytes()); err != nil {
						break
					}
					n++
				}
			}
			atomic.AddUint32(&sended, n)
		}()

		req, err := http.NewRequest("POST", d.query, pr)
		if err != nil {
			return 0, 0, err
		}

		client := &http.Client{
			Timeout:   time.Second * 60,
			Transport: &http.Transport{DisableKeepAlives: true},
		}
		resp, err := client.Do(req)
		if err != nil {
			return time.Since(start), uint(atomic.LoadUint32(&sended)), err
		}
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)

		if resp.StatusCode != 200 {
			return time.Since(start), uint(atomic.LoadUint32(&sended)), fmt.Errorf("clickhouse response status %d: %s", resp.StatusCode, string(body))
		}

		d.metrics = d.metrics[:0]
		d.size = 0
	}
	return time.Since(start), uint(atomic.LoadUint32(&sended)), nil
}

func (d *TaggedDriver) Close() error {
	return nil
}
