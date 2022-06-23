package RowBinary

import (
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/msaf1980/go-stringutils"
)

const NullUint32 = ^uint32(0)

func DateToUint16(t time.Time) uint16 {
	return uint16(time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

func Date(year int, month time.Month, day int) uint16 {
	return uint16(time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix() / 86400)
}

type Writer struct {
	wrapped io.Writer
	buffer  [265]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{wrapped: w}
}

func (w *Writer) WriteDate(value time.Time) error {
	return w.WriteUint16(DateToUint16(value))
}

func (w *Writer) WriteUint8(value uint8) error {
	_, err := w.wrapped.Write([]byte{value})
	return err
}

func (w *Writer) WriteUint16(value uint16) error {
	binary.LittleEndian.PutUint16(w.buffer[:], value)
	_, err := w.wrapped.Write(w.buffer[:2])
	return err
}

func (w *Writer) WriteUint32(value uint32) error {
	binary.LittleEndian.PutUint32(w.buffer[:], value)
	_, err := w.wrapped.Write(w.buffer[:4])
	return err
}

func (w *Writer) WriteNullableUint32(value uint32) error {
	if value == NullUint32 {
		_, err := w.wrapped.Write([]byte{1})
		return err
	}
	_, err := w.wrapped.Write([]byte{0})
	if err != nil {
		return err
	}
	return w.WriteUint32(value)
}

func (w *Writer) WriteUint64(value uint64) error {
	binary.LittleEndian.PutUint64(w.buffer[:], value)
	_, err := w.wrapped.Write(w.buffer[:8])
	return err
}

func (w *Writer) WriteFloat64(value float64) error {
	return w.WriteUint64(math.Float64bits(value))
}

func (w *Writer) WriteNullableFloat64(value float64) error {
	if math.IsNaN(value) {
		_, err := w.wrapped.Write([]byte{1})
		return err
	}
	_, err := w.wrapped.Write([]byte{0})
	if err != nil {
		return err
	}
	return w.WriteFloat64(value)
}

func (w *Writer) WriteUvarint(v uint64) (int, error) {
	n := binary.PutUvarint(w.buffer[:], v)
	return w.wrapped.Write(w.buffer[:n])
}

func (w *Writer) WriteBytes(value []byte) error {
	_, err := w.WriteUvarint(uint64(len(value)))
	if err != nil {
		return err
	}

	_, err = w.wrapped.Write(value)
	return err
}

func (w *Writer) WriteString(value string) error {
	return w.WriteBytes(stringutils.UnsafeStringBytes(&value))
}

func (w *Writer) WriteStringList(value []string) error {
	_, err := w.WriteUvarint(uint64(len(value)))
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.WriteString(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) Uint32List(value []uint32) error {
	_, err := w.WriteUvarint(uint64(len(value)))
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.WriteUint32(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) NullableUint32List(value []uint32) error {
	_, err := w.WriteUvarint(uint64(len(value)))
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.WriteNullableUint32(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) Float64List(value []float64) error {
	_, err := w.WriteUvarint(uint64(len(value)))
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.WriteFloat64(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) NullableFloat64List(value []float64) error {
	_, err := w.WriteUvarint(uint64(len(value)))
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i++ {
		err = w.WriteNullableFloat64(value[i])
		if err != nil {
			return err
		}
	}

	return nil
}
