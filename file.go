package main

import (
	"bufio"
	"compress/gzip"
	"os"
	"strings"
)

func openFile(filename string) (*bufio.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var reader *bufio.Reader

	if strings.HasSuffix(filename, ".gz") {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		reader = bufio.NewReader(gzipReader)
	} else {
		reader = bufio.NewReader(file)
	}

	return reader, nil
}
