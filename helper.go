package main

import (
	"sync"
	"time"
	"expvar"
	"github.com/uber-go/zap"
	"unsafe"
	"sync/atomic"
)

var logger zap.Logger

// Config is a structure with general configuration data
var Config = struct {
	Senders                int
	Endpoint               string
	GraphiteDB             string
	GraphiteTreeDB         string
	Interval               time.Duration
	ClickhouseSendInterval int
	QueueLimitElements     int
	GraphiteHost           string
	ResetMetrics           bool
}{
	Senders:                4,
	Endpoint:               "http://localhost:8123",
	GraphiteDB:             "graphite",
	GraphiteTreeDB:         "graphite_tree",
	Interval:               60 * time.Second,
	ClickhouseSendInterval: 10000,
	QueueLimitElements:     3000000,
	GraphiteHost:           "",
	ResetMetrics:           false,
}


// Metrics is a structure that store all internal metrics
var Metrics = struct {
	MetricsReceived    *expvar.Int
	MetricsSent        *expvar.Int
	MetricsErrors      *expvar.Int
	MetricsDropped     *expvar.Int
	ReceiveErrors      *expvar.Int
	ParseErrors        *expvar.Int
	QueueErrors        *expvar.Int
	SendErrors         *expvar.Int
	SendTimeNS         *expvar.Int
	SendRequests       *expvar.Int
	QueueSize          *expvar.Int
	TreeUpdates        *expvar.Int
	TreeUpdateErrors   *expvar.Int
	TreeUpdateRequests *expvar.Int
}{
	MetricsReceived:    expvar.NewInt("metrics_received"),
	MetricsSent:        expvar.NewInt("metrics_sent"),
	MetricsErrors:      expvar.NewInt("metrics_errors"),
	MetricsDropped:     expvar.NewInt("metrics_dropped"),
	ParseErrors:        expvar.NewInt("parse_errors"),
	ReceiveErrors:      expvar.NewInt("receive_errors"),
	QueueErrors:        expvar.NewInt("queue_errors"),
	SendErrors:         expvar.NewInt("send_errors"),
	SendTimeNS:         expvar.NewInt("send_time_ns"),
	SendRequests:       expvar.NewInt("send_requests"),
	QueueSize:          expvar.NewInt("queue_size"),
	TreeUpdates:        expvar.NewInt("tree_updates"),
	TreeUpdateErrors:   expvar.NewInt("tree_update_errors"),
	TreeUpdateRequests: expvar.NewInt("tree_update_requests"),
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

type queue struct {
	sync.RWMutex
	data [][]byte
}

var queues []queue

var writerTime int64

func timeUpdater(exit <-chan struct{}) {
	tick := time.Tick(1 * time.Second)
	ticks := 0
	for {
		select {
		case <-exit:
			return
		case <-tick:
			if ticks < 60*60 {
				atomic.AddInt64(&writerTime, 1)
			} else {
				currentTime := time.Now().Unix()
				atomic.StoreInt64(&writerTime, currentTime)
				ticks = 0
			}
			ticks++

		}
	}
}

func flushMetrics() {
	Metrics.MetricsReceived.Set(0)
	Metrics.MetricsErrors.Set(0)
	Metrics.MetricsDropped.Set(0)
	Metrics.ParseErrors.Set(0)
	Metrics.QueueErrors.Set(0)
	Metrics.ReceiveErrors.Set(0)
	Metrics.SendErrors.Set(0)
	Metrics.SendRequests.Set(0)
	Metrics.MetricsSent.Set(0)
	Metrics.SendTimeNS.Set(0)
}

func stats(exit <-chan struct{}) {
	tick := time.Tick(Config.Interval)
	for {
		select {
		case <-exit:
			return
		case <-tick:
			cnt := int64(0)
			for idx := range queues {
				cnt += int64(len(queues[idx].data))
			}
			Metrics.QueueSize.Set(cnt)

			if Config.GraphiteHost == "" {
				logger.Info("Stats",
					zap.String("metrics_received", Metrics.MetricsReceived.String()),
					zap.String("metrics_errors", Metrics.MetricsErrors.String()),
					zap.String("metrics_dropped", Metrics.MetricsDropped.String()),
					zap.String("metrics_sent", Metrics.MetricsSent.String()),
					zap.String("parse_errors", Metrics.ParseErrors.String()),
					zap.String("queue_errors", Metrics.QueueErrors.String()),
					zap.String("receive_errors", Metrics.ReceiveErrors.String()),
					zap.String("send_errors", Metrics.SendErrors.String()),
					zap.String("send_requests", Metrics.SendRequests.String()),
					zap.String("send_time_ns", Metrics.SendTimeNS.String()),
					zap.Int64("queue_size", cnt),
				)
			}
			if Config.ResetMetrics {
				flushMetrics()
			}
			//			logfile.Sync()
		}
	}

}
