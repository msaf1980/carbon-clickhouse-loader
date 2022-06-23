package RowBinary

import (
	"encoding/binary"
	"io"
	"math"
	"time"
)

type Reader struct {
	wrapped io.Reader

	start int
	end   int
	buf   []byte
}

func NewReaderBuffered(rdr io.Reader, bufSize int) *Reader {
	if bufSize < SIZE_INT64 {
		bufSize = SIZE_INT64
	}
	return &Reader{
		wrapped: rdr,
		buf:     make([]byte, bufSize),
	}
}

func (r *Reader) grow(n int) {
	if n > r.end-r.start {
		tmp := r.buf
		r.buf = make([]byte, n)
		copy(r.buf, tmp[r.start:r.end])
		if r.start > 0 {
			r.end -= r.start
			r.start = 0
		}
	}
}

func (r *Reader) read(want int) (int, []byte, error) {
	size := r.end - r.start
	if size >= want {
		// all in buffer
		start := r.start
		r.start += want
		return want, r.buf[start:r.start], nil
	}

	newSize := r.start + want
	if newSize > cap(r.buf) {
		// buffer need to grow
		r.grow(newSize)
	}

	if n, err := r.wrapped.Read(r.buf[r.end:]); err != nil {
		return n, nil, err
	} else {
		r.end += n
		if r.end-r.start < want {
			return n, nil, ErrEOF
		} else {
			start := r.start
			r.start += want
			return n, r.buf[start:r.start], nil
		}
	}
}

func (r *Reader) readUvarint() (uint64, error) {
	if r.start < r.end {
		// try to read from buffer
		if u, n, err := readUvarint(r.buf[r.start:r.end]); err == nil {
			r.start += n
			return u, nil
		} else if err != ErrEOF {
			return u, err
		}
	}

	newSize := r.start + SIZE_INT64
	if newSize > cap(r.buf) {
		// buffer need to grow
		r.grow(newSize)
	}

	if n, err := r.wrapped.Read(r.buf[r.end:]); err != nil {
		return 0, err
	} else {
		r.end += n
		if u, n, err := readUvarint(r.buf[r.start:r.end]); err == nil {
			r.start += n
			return u, nil
		} else {
			return u, err
		}
	}
}

func (r *Reader) ReadUint8() (uint8, error) {
	if _, buf, err := r.read(SIZE_INT8); err != nil {
		return 0, err
	} else {
		return buf[0], nil
	}
}

func (r *Reader) ReadUint16() (uint16, error) {
	if _, buf, err := r.read(SIZE_INT16); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint16(buf), nil
	}
}

func (r *Reader) ReadUint32() (uint32, error) {
	if _, buf, err := r.read(SIZE_INT32); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint32(buf), nil
	}
}

func (r *Reader) ReadUint64() (uint64, error) {
	if _, buf, err := r.read(SIZE_INT64); err != nil {
		return 0, err
	} else {
		return binary.LittleEndian.Uint64(buf), nil
	}
}

func (r *Reader) ReadFloat64() (float64, error) {
	if _, buf, err := r.read(SIZE_INT64); err != nil {
		return 0, err
	} else {
		return math.Float64frombits(binary.LittleEndian.Uint64(buf)), nil
	}
}

func (r *Reader) ReadString() (string, error) {
	if u, err := r.readUvarint(); err != nil {
		return "", err
	} else {
		if u == 0 {
			return "", nil
		} else if _, buf, err := r.read(int(u)); err != nil {
			return "", ErrEOF
		} else {
			return string(buf), nil
		}
	}
}

func (r *Reader) ReadDate() (time.Time, error) {
	if t, err := r.ReadUint16(); err != nil {
		return time.Unix(0, 0), err
	} else {
		return DateUint16(t), nil
	}
}

func (r *Reader) ReadStringList() ([]string, error) {
	if u, err := r.readUvarint(); err != nil {
		return nil, err
	} else {
		if u == 0 {
			return []string{}, nil
		}
		n := int(u)
		sList := make([]string, n)
		for i := 0; i < n; i++ {
			if sList[i], err = r.ReadString(); err != nil {
				return sList, err
			}
		}
		return sList, nil
	}
}
