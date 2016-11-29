package main

import (
	"flag"
	"fmt"
	g2g "github.com/peterbourgon/g2g"
	"github.com/uber-go/zap"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// TODO: Main is a mess now, clean it up
func main() {
	var err error
	listen := flag.String("l", ":2003", "Graphite Line Protocol listener")
	endpoint := flag.String("e", Config.Endpoint, "Clickhouse-compatible endpoint")
	interval := flag.Duration("i", Config.Interval, "Interval for internal stats")
	senders := flag.Int("s", Config.Senders, "Amount of concurent senders")
	batch := flag.Int("b", Config.ClickhouseSendInterval, "Amount of points that'll be sent to clickhouse in one request")
	graphiteHost := flag.String("g", Config.GraphiteHost, "Graphite host to send own metrics")
	queueLimit := flag.Int("q", Config.QueueLimitElements, "Amount of elements in queue per sender before we'll drop the data")
	resetMetrics := flag.Bool("r", Config.ResetMetrics, "Reset metrics after each send (gauge vs counter)")
	// logFileName := flag.String("o", "g2c.log", "Log file")
	flag.Parse()

	Config.Endpoint = *endpoint
	Config.Interval = *interval
	Config.ClickhouseSendInterval = *batch
	Config.QueueLimitElements = *queueLimit
	Config.ResetMetrics = *resetMetrics
	Config.GraphiteHost = *graphiteHost
	Config.Senders = *senders

	GraphiteTreeDBEndpoint = Config.Endpoint + "/db/" + Config.GraphiteTreeDB + "/series"

	/*
		logfile, err = os.OpenFile(*logFileName, os.O_APPEND|os.O_CREATE, 0644)
		if err != nil {
			logfile, err = os.Create(*logFileName)
			if err != nil {
				log.Fatalf("Failed to open or create file: %v", err)
			}
		}
		defer logfile.Close()
	*/
	log.Println("g2c")
	logger = zap.New(zap.NewTextEncoder())
	logger.Info("Starting...")

	go func() {
		err = http.ListenAndServe(":80", nil)
		if err != nil {
			logger.Error("Failed to start pprof http server on :80", zap.Error(err))
		}
	}()

	exit := make(chan struct{})

	currentTime := uint32(time.Now().Unix())
	atomic.StoreUint32(&writerTime, currentTime)
	go timeUpdater(exit)

	if *graphiteHost != "" {
		logger.Info("Will send data to graphite", zap.String("host", *graphiteHost))
		graphite := g2g.NewGraphite(*graphiteHost, Config.Interval, 10*time.Second)

		hostname, err := os.Hostname()
		if err != nil {
			logger.Fatal("Can't get hostname", zap.Error(err))
		}
		hostname = strings.Replace(hostname, ".", "_", -1)

		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.metrics_received", hostname), Metrics.MetricsReceived)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.metrics_errors", hostname), Metrics.MetricsErrors)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.metrics_dropped", hostname), Metrics.MetricsDropped)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.metrics_sent", hostname), Metrics.MetricsSent)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.parse_errors", hostname), Metrics.ParseErrors)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.queue_errors", hostname), Metrics.QueueErrors)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.receive_errors", hostname), Metrics.ReceiveErrors)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.send_errors", hostname), Metrics.SendErrors)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.send_requests", hostname), Metrics.SendRequests)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.send_time_ns", hostname), Metrics.SendTimeNS)
		graphite.Register(fmt.Sprintf("carbon.clickhouse.g2c.%s.queue_size", hostname), Metrics.QueueSize)
	} else {
		logger.Info("Will print my own stats to console")
	}
	go stats(exit)

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatal("Can't open graphite compatible listener port", zap.Error(err))
	}

	queues = make([]queue, *senders)
	metricsTreeUpdateQueues = make([]queue, *senders)
	//metricsTree = radix.New()
	metricsTree = make(map[string]int, 1000000)
	go metricsTreeUpdater()
	for i := 0; i < *senders; i++ {
		go clickHouseWriter(i)
	}

	logger.Info("Started")
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Error("Failed to accept connection", zap.Error(err))
			continue
		}

		go processGraphite(conn)
	}
}
