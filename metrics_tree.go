package main

import (
	"bytes"
	"github.com/uber-go/zap"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var metricsTree map[string]int
var metricsTreeUpdateQueues []queue

var GraphiteTreeDBEndpoint string

var treeNeedsUpdate int64

func metricsTreeUpdater() {
	var updateList [][]byte
	var ok bool
	header := []byte("insert into " + Config.GraphiteTreeDB + " format TabSeparated\n")
	buffer := bytes.NewBuffer(header)
	ts := atomic.LoadUint32(&writerTime)
	prevTs := ts
	date := []byte(time.Unix(int64(ts), 0).Format("2006-01-02"))

	client := http.Client{
		// TODO: Remove hardcoded sleep time
		Timeout: 15 * time.Second,
	}
	sentNames := 0
	for {
		haveWork := atomic.CompareAndSwapInt64(&treeNeedsUpdate, 1, 0)
		if !haveWork {
			time.Sleep(1 * time.Second)
			continue
		}
		logger.Info("I have work!")

		for number := range metricsTreeUpdateQueues {
			metricsTreeUpdateQueues[number].Lock()
			updateList = append(updateList, metricsTreeUpdateQueues[number].data...)
			metricsTreeUpdateQueues[number].data = metricsTreeUpdateQueues[number].data[:0]
			metricsTreeUpdateQueues[number].Unlock()
		}

		if len(updateList) == 0 {
			// We want faster reaction on updates, so sleep time here is different
			// TODO: Remove hardcoded sleep time
			time.Sleep(1 * time.Second)
			continue
		}
		logger.Info("updateList", zap.Int("len", len(updateList)))
		prefixList := make(map[string]int)

		ts = atomic.LoadUint32(&writerTime)
		if ts != prevTs {
			prevTs = ts
			date = []byte(time.Unix(int64(ts), 0).Format("2006-01-02"))
		}

		for _, metric := range updateList {
			level := 1
			// unsafeString is "safe to use" here at least as of Go 1.7 version.
			// Due to internal hashmap implementation and to the fact that prefixList
			// is short-living object, metric will still be here and won't be collected by GC
			_, ok = prefixList[unsafeString(metric)]
			if ok {
				continue
			}
			prefixList[unsafeString(metric)] = 1
			for idx := range metric {
				if metric[idx] == '.' {
					if idx != len(metric) {
						idx++
					}
					_, ok = prefixList[unsafeString(metric[:idx])]
					if ok {
						level++
						continue
					}
					// TODO: Generalize this code with a 'buffer.Write' block below
					prefixList[unsafeString(metric[:idx])] = 1
					buffer.Write(date)
					buffer.WriteByte('\t')
					buffer.Write([]byte(strconv.Itoa(level)))
					buffer.WriteByte('\t')
					buffer.Write(metric[:idx])
					buffer.WriteByte('\n')
					level++
					sentNames++
				}
			}
			buffer.Write(date)
			buffer.WriteByte('\t')
			buffer.Write([]byte(strconv.Itoa(level)))
			buffer.WriteByte('\t')
			buffer.Write(metric)
			buffer.WriteByte('\n')
		}
		updateList = updateList[:0]
		err := sendData(&client, GraphiteTreeDBEndpoint, buffer)
		if err != nil {
			logger.Error("Can't send data to Clickhouse", zap.Error(err))
			Metrics.TreeUpdateErrors.Add(1)
		} else {
			Metrics.TreeUpdateRequests.Add(1)
			Metrics.TreeUpdates.Add(int64(sentNames))
			sentNames = 0
		}
		if buffer.Len() > 0 {
			// TODO: We should maintain buffer if Clickhouse is not available
			logger.Error("Buffer is not empty. Handling this situation is not implemented yet")
			buffer.Reset()
			buffer.Write(header)
			// TODO: Remove hardcoded sleep time
			time.Sleep(60 * time.Second)
			continue
		}
		buffer.Write(header)

		// TODO: Remove hardcoded sleep time
		time.Sleep(60 * time.Second)
	}
}
