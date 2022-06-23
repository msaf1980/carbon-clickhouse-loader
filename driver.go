package main

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/msaf1980/carbon-clickhouse-loader/pkg/driver"
	driver_mail_ru "github.com/msaf1980/carbon-clickhouse-loader/pkg/driver/mail_ru"
	driver_native "github.com/msaf1980/carbon-clickhouse-loader/pkg/driver/native"
	driver_rowbin "github.com/msaf1980/carbon-clickhouse-loader/pkg/driver/rowbin"
	driver_std "github.com/msaf1980/carbon-clickhouse-loader/pkg/driver/std"
	"github.com/tevino/abool/v2"
)

type MetricIndexStore struct {
	taggedDriver driver.Driver
	taggedCh     chan driver.MetricIndex
	// tagedStopCh  chan struct{}
	stopWG    sync.WaitGroup
	isRunning *abool.AtomicBool
}

func (bg *MetricIndexStore) spawnTagged() {
	bg.stopWG.Add(1)
	defer bg.stopWG.Done()

	for m := range bg.taggedCh {
		if bg.isRunning.IsNotSet() {
			break
		}
		if d, n, err := bg.taggedDriver.Write(m); err != nil {
			log.Printf("ERROR (%v): %v", d, err)
		} else if n > 0 {
			log.Printf("FLUSH (%v): %d", d, n)
		}
	}

	// for m := range bg.taggedCh {
	// 	if d, n, err := bg.taggedDriver.Write(m); err != nil {
	// 		log.Printf("ERROR (%v): %v", d, err)
	// 		break
	// 	} else if n > 0 {
	// 		log.Printf("FLUSH (%v): %d", d, n)
	// 	}
	// }

	if d, n, err := bg.taggedDriver.Flush(); err != nil {
		log.Printf("ERROR (%v): %v", d, err)
	} else if n > 0 {
		log.Printf("FLUSH (%v): %d", d, n)
	}
}

func (bg *MetricIndexStore) Push(m driver.MetricIndex) {
	if strings.Contains(m.Metric, ";") {
		// tagged metric
		if bg.taggedDriver != nil {
			bg.taggedCh <- m
		}
	}
	// TODO: plain metric
}

func (bg *MetricIndexStore) Interrupt() {
	bg.isRunning.UnSet()
	if bg.taggedDriver != nil {
		close(bg.taggedCh)
	}
	bg.stopWG.Wait()
}

func (bg *MetricIndexStore) Stop() {
	if bg.taggedDriver != nil {
		close(bg.taggedCh)
	}
	bg.stopWG.Wait()
}

func (bg *MetricIndexStore) FlushInit() {
	bg.Push(driver.MetricIndex{})
}

func NewMetricIndexStore(chDriver ChDriver, address, plainTable, taggedTable string, flushSize uint, isRunning *abool.AtomicBool) (*MetricIndexStore, error) {
	var taggedDriver driver.Driver
	switch chDriver {
	case ChDriverMailRu:
		if len(taggedTable) > 0 {
			taggedDriver = driver_mail_ru.NewTaggedDriver(address, taggedTable, flushSize)
		}
	case ChDriverStd:
		if len(taggedTable) > 0 {
			taggedDriver = driver_std.NewTaggedDriver(address, taggedTable, flushSize)
		}
	case ChDriverNative:
		if len(taggedTable) > 0 {
			taggedDriver = driver_native.NewTaggedDriver(address, taggedTable, flushSize)
		}
	case ChDriverRowBinary:
		if len(taggedTable) > 0 {
			taggedDriver = driver_rowbin.NewTaggedDriver(address, taggedTable, flushSize)
		}
	default:
		return nil, fmt.Errorf("driver not supported: %s", chDriver.String())
	}

	drv := &MetricIndexStore{
		taggedDriver: taggedDriver,
		taggedCh:     make(chan driver.MetricIndex, 100),
		// tagedStopCh:  make(chan struct{}),
		isRunning: isRunning,
	}

	go drv.spawnTagged()

	return drv, nil
}
