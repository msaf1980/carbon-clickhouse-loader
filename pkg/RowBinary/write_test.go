package RowBinary

import (
	"bytes"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWriteUint8(t *testing.T) {
	tests := []uint8{0, 126, math.MaxUint8}
	for _, tt := range tests {
		t.Run(strconv.FormatUint(uint64(tt), 10), func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteUint8(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadUint8()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint8()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint8(0), n)
		})
	}
}

func TestWriteUint16(t *testing.T) {
	tests := []uint16{0, 126, 256, 1025, math.MaxUint16}
	for _, tt := range tests {
		t.Run(strconv.FormatUint(uint64(tt), 10), func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteUint16(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadUint16()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}

func TestWriteUint32(t *testing.T) {
	tests := []uint32{0, 126, 256, 1025, math.MaxUint16, math.MaxUint32}
	for _, tt := range tests {
		t.Run(strconv.FormatUint(uint64(tt), 10), func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteUint32(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadUint32()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}

func TestWriteUint64(t *testing.T) {
	tests := []uint64{0, 126, 256, 1025, math.MaxUint16, math.MaxUint32, math.MaxUint64}
	for _, tt := range tests {
		t.Run(strconv.FormatUint(uint64(tt), 10), func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteUint64(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadUint64()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}

func TestWriteFloat64(t *testing.T) {
	tests := []float64{0.0, 126.0, 256.0, 1025.0, float64(math.MaxUint16), float64(math.MaxUint32), math.MaxFloat64}
	for _, tt := range tests {
		t.Run(strconv.FormatUint(uint64(tt), 10), func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteFloat64(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadFloat64()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}

func TestWriteDate(t *testing.T) {
	tests := []time.Time{
		time.Date(1970, time.December, 26, 0, 0, 0, 0, time.UTC),
		time.Date(1996, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, tt := range tests {
		t.Run(tt.String(), func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteDate(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadDate()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}

func TestWriteString(t *testing.T) {
	tests := []string{
		"",
		"test",
		"test long string 123456789 test long string 123456789 test long string 123456789 test long string 123456789 test long string 123456789 test long string 123456789",
	}
	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteString(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadString()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}

func TestWriteStringList(t *testing.T) {
	tests := [][]string{
		{},
		{""},
		{"test"},
		{"test long string 123456789 test long string 123456789 test long string 123456789 test long string 123456789 test long string 123456789 test long string 123456789"},
	}
	for _, tt := range tests {
		t.Run("["+strings.Join(tt, ",")+"]", func(t *testing.T) {
			wb := &bytes.Buffer{}
			w := NewWriter(wb)
			err := w.WriteStringList(tt)
			assert.NoError(t, err)

			r := NewReaderBuffered(bytes.NewReader(wb.Bytes()), 0)
			got, err := r.ReadStringList()
			assert.NoError(t, err)
			assert.Equal(t, tt, got)

			// want EOF
			n, err := r.ReadUint16()
			assert.Equal(t, io.EOF, err)
			assert.Equal(t, uint16(0), n)
		})
	}
}
