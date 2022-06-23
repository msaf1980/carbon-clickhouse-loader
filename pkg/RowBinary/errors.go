package RowBinary

import "errors"

var ErrEOF = errors.New("unexcepted end")
var ErrUvarintOverflow = errors.New("varint overflow")
