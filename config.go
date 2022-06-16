package main

import (
	"fmt"
	"strings"
)

type ChDriver int16

const (
	ChDriverRowBinary ChDriver = iota
	ChDriverMailRu
	ChDriverStd    // clickhouse-go with database/sql interface
	ChDriverNative // clickhouse-go with native interface
)

var driverStrings []string = []string{"rowbin", "mail.ru", "std", "native"}

func (a *ChDriver) Set(value string) error {
	switch value {
	case "rowbin", "rowbinary":
		*a = ChDriverRowBinary
	case "mail.ru":
		*a = ChDriverMailRu
	case "std":
		*a = ChDriverStd
	case "native":
		*a = ChDriverNative
	default:
		return fmt.Errorf("invalid clickhouse driver %s", value)
	}
	return nil
}

// UnmarshalYAML for use Aggregation in yaml files
func (a *ChDriver) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var value string
	if err := unmarshal(&value); err != nil {
		return err
	}

	if err := a.Set(value); err != nil {
		return fmt.Errorf("failed to parse '%s' to Driver: %v", value, err)
	}

	return nil
}

func (a *ChDriver) String() string {
	return driverStrings[*a]
}

func (a *ChDriver) Type() string {
	return "driver"
}

func (a *ChDriver) Drivers() string {
	return "[" + strings.Join(driverStrings, ",") + "]"
}

type StringSlice []string

func (u *StringSlice) Set(value string) error {
	*u = append(*u, value)
	return nil
}

func (u *StringSlice) String() string {
	return "[ " + strings.Join(*u, ", ") + " ]"
}

func (u *StringSlice) Type() string {
	return "[]string"
}
