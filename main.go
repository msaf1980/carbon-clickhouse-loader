package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/msaf1980/carbon-clickhouse-loader/pkg/driver"
	flag "github.com/spf13/pflag"
	"github.com/tevino/abool/v2"
)

func main() {
	var fileNames StringSlice
	flag.VarP(&fileNames, "file", "f", "metrics file")

	// var chURL *string = flag.StringP("url", "u", "", "clickhouse URL")
	var chDriver ChDriver
	flag.VarP(&chDriver, "driver", "d", fmt.Sprintf("clickhouse driver %s", chDriver.Drivers()))

	var chunkSize driver.Size = 1024 * 1024
	flag.VarP(&chunkSize, "size", "s", "metrics chunk max size (by default 1M)")
	if chunkSize < 1 {
		chunkSize = 1
	}

	// indexTable := flag.StringP("index", "i", "", "graphite index table")
	taggedTable := flag.StringP("tagged", "t", "", "graphite tagged table")

	address := flag.StringP("address", "a", "", "clickhouse address")

	flag.Parse()

	var ec int
	isRunning := abool.NewBool(true)

	store, err := NewMetricIndexStore(chDriver, *address, "", *taggedTable, uint(chunkSize), isRunning)
	if err != nil {
		log.Fatalf("error creating store: %v", err)
	}

	dates := []time.Time{time.Now()}

	termCh := make(chan os.Signal)
	signal.Notify(termCh, syscall.SIGTERM, syscall.SIGINT)

	go func(isRunning *abool.AtomicBool) {
		<-termCh // Blocks here until interrupted
		isRunning.UnSet()
	}(isRunning)

MAIN_LOOP:
	for _, date := range dates {
		for _, filename := range fileNames {
			reader, err := openFile(filename)
			if err != nil {
				log.Printf("error opening file: %v", err)
				ec = 1
				continue
			}

			log.Printf("read file: %s", filename)
			n := 0
			for {
				if isRunning.IsNotSet() {
					log.Print("interrupted\n")
					break MAIN_LOOP
				}
				n++
				line, err := reader.ReadString('\n')
				if err != nil {
					break MAIN_LOOP
				}
				metric := strings.TrimRight(line, "\n")
				if len(metric) > 0 {
					store.Push(driver.MetricIndex{
						Metric: string(metric),
						Date:   date,
					})
				}
			}
		}

		store.FlushInit()
	}

	store.Stop()

	os.Exit(ec)
}
