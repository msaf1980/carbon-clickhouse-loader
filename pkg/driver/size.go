package driver

import (
	"fmt"
	"strconv"
	"strings"
)

type Size int

func (u *Size) Set(value string) error {
	var err error
	var s int64

	value = strings.ToLower(value)
	last := len(value) - 1
	suffix := value[last]
	switch suffix {
	case 'k':
		s, err = strconv.ParseInt(value[0:last], 10, 64)
		s *= 1024
	case 'm':
		s, err = strconv.ParseInt(value[0:last], 10, 64)
		s *= 1024 * 1024
	case 'g':
		s, err = strconv.ParseInt(value[0:last], 10, 64)
		s *= 1024 * 1024 * 1024
	default:
		s, err = strconv.ParseInt(value, 10, 64)
	}

	if s < 0 {
		err = fmt.Errorf("size must be greater than 0")
	}
	*u = Size(s)
	return err
}

func (u *Size) Value() int {
	return int(*u)
}

func (u *Size) String() string {
	return strconv.Itoa(int(*u))
}

func (u *Size) Type() string {
	return "size"
}
