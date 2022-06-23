package RowBinary

import "time"

const (
	SIZE_INT8  = 1
	SIZE_INT16 = 2
	SIZE_INT32 = 4
	SIZE_INT64 = 8
)

func DateUint16(n uint16) time.Time {
	return time.Unix(int64(n)*86400, 0).UTC()
}

func readUvarint(array []byte) (uint64, int, error) {
	var x uint64
	var s uint
	l := len(array) - 1
	for i := 0; ; i++ {
		if i > l {
			return x, i + 1, ErrEOF
		}
		if array[i] < 0x80 {
			if i > 9 || i == 9 && array[i] > 1 {
				return x, i + 1, ErrUvarintOverflow
			}
			return x | uint64(array[i])<<s, i + 1, nil
		}
		x |= uint64(array[i]&0x7f) << s
		s += 7
	}
}
