package main

import (
	"bytes"
	"github.com/uber-go/zap"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

var GraphiteDBEndpoint string

func clickHouseWriter(number int) {
	sentMetrics := 0
	header := []byte("insert into graphite format TabSeparated\n")
	ts := atomic.LoadInt64(&writerTime)
	prevTs := ts
	date := []byte(time.Unix(ts, 0).Format("2006-01-02"))
	version := []byte(strconv.Itoa(int(ts)))

	buffer := bytes.NewBuffer(header)
	sleepTime := time.Duration(2*1000/Config.Senders) * time.Millisecond
	client := http.Client{
		Timeout: 15 * time.Second,
	}
	metricsList := make(map[string]int)
	newMetricsQueue := make([][]byte, 0, 10000)
	var data [][]byte
	for {
		sendStartTime := time.Now()
		queues[number].Lock()
		data = queues[number].data
		queues[number].data = make([][]byte, 0, len(data))
		queues[number].Unlock()

		for _, line := range data {
			ts = atomic.LoadInt64(&writerTime)
			if ts != prevTs {
				prevTs = ts
				date = []byte(time.Unix(ts, 0).Format("2006-01-02"))
				version = []byte(strconv.Itoa(int(ts)))
			}

			idx := bytes.IndexByte(line, ' ')
			name := line[:idx]
			// As of Go 1.7 unsafeString should be safe to use here
			// TODO: Try to find elss uglier solution that accepts []byte
			// TODO: Speed up this code
			_, ok := metricsList[unsafeString(name)]
			if !ok {
				metricsList[unsafeString(name)] = 1
				newMetricsQueue = append(newMetricsQueue, name)
			}
			err := preparePoint(line, buffer, date, version, number)
			if err != nil {
				logger.Error("Failed to parse graphite line", zap.String("line", string(line)), zap.Error(err))
				Metrics.ParseErrors.Add(1)
				continue
			}
			Metrics.MetricsReceived.Add(1)
			sentMetrics++
		}

		if buffer.Len() > len(header) {
			// We don't want to lock mutex if we don't need to
			if len(newMetricsQueue) > 0 {
				metricsTreeUpdateQueues[number].Lock()
				metricsTreeUpdateQueues[number].data = append(metricsTreeUpdateQueues[number].data, newMetricsQueue...)
				metricsTreeUpdateQueues[number].Unlock()
				newMetricsQueue = newMetricsQueue[:0]
				atomic.StoreInt64(&treeNeedsUpdate, 1)
			}

			err := sendData(&client, GraphiteDBEndpoint, buffer)
			if err != nil {
				logger.Error("Can't send data to Clickhouse", zap.Error(err))
				Metrics.SendErrors.Add(1)
			} else {
				Metrics.SendRequests.Add(1)
				Metrics.MetricsSent.Add(int64(sentMetrics))
				sentMetrics = 0
			}
			if buffer.Len() > 0 {
				logger.Error("Buffer is not empty. Handling this situation is not implemented yet")
				buffer.Reset()
			}
			buffer.Write(header)
		}

		waitTime := time.Since(sendStartTime)
		Metrics.SendTimeNS.Add(waitTime.Nanoseconds())
		// We are trying to perform not more than 10 POST requests per second
		if waitTime < sleepTime {
			time.Sleep(sleepTime - waitTime)
		}
	}
}
