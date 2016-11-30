package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/uber-go/zap"
	"hash/fnv"
	"io"
	"math"
	"net"
	"strconv"
	"time"
)

var errParseName = errors.New("Can't parse name")
var errParseValue = errors.New("Can't parse value")

func parsePoint(line []byte) ([]byte, float64, uint32, error) {
	s1 := bytes.IndexByte(line, ' ')
	// Some sane limit
	if s1 < 1 || s1 > 1024*1024 {
		return nil, 0, 0, errParseName
	}
	s2 := bytes.IndexByte(line[s1+1:], ' ')
	if s2 < 1 {
		return nil, 0, 0, errParseValue
	}
	s2 += s1 + 1

	value, err := strconv.ParseFloat(unsafeString(line[s1+1:s2]), 64)
	if err != nil || math.IsNaN(value) {
		logger.Error("Can't parse value", zap.Error(err))
		return nil, 0, 0, errParseValue
	}
	s3 := len(line) - 1

	ts, err := strconv.ParseFloat(unsafeString(line[s2+1:s3]), 64)
	if err != nil || math.IsNaN(ts) || math.IsInf(ts, 0) {
		logger.Error("Can't parse timestamp", zap.Error(err))
		return nil, 0, 0, errParseTimestamp
	}

	return line[:s1], value, uint32(ts), nil
}

// TODO: Make a module out of this code
func preparePoint(line []byte, buffer io.Writer, version uint32, days *DaysFrom1970, number int) ([]byte, error) {
	name, value, ts, err := parsePoint(line)
	if err != nil {
		return nil, err
	}

	/* Graphite Tree
	 *
	 * As of Go 1.7 unsafeString should be safe to use here
	 * TODO: Try to find elss uglier solution that accepts []byte
	 * TODO: Speed up this code
	 */
	var tmp [8]byte
	bytesWritten := binary.PutUvarint(tmp[:4], uint64(len(name)))

	buffer.Write(tmp[:bytesWritten])
	buffer.Write(name)

	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(value))
	buffer.Write(tmp[:])

	binary.LittleEndian.PutUint32(tmp[:4], ts)
	buffer.Write(tmp[:4])

	date := days.TimestampWithNow(ts, version)
	binary.LittleEndian.PutUint16(tmp[:2], date)
	buffer.Write(tmp[:2])

	binary.LittleEndian.PutUint32(tmp[:4], version)
	buffer.Write(tmp[:4])
	return name, nil

}

func processGraphite(c net.Conn) {
	defer func() {
		err := c.Close()
		if err != nil {
			logger.Error("Can't close connection", zap.Error(err))
		}
	}()

	reader := bufio.NewReaderSize(c, 32*1024)
	metricsPending := 0
	// TODO: This is not working at all, replace it with better buffers.
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
	dataBuffer := make([][][]byte, Config.Senders)
	hash := fnv.New32a()
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				if len(line) > 0 {
					Metrics.ReceiveErrors.Add(1)
					logger.Error("Unfinished line: ", zap.String("line", string(line)))
				}
				break
			} else {
				Metrics.ReceiveErrors.Add(1)
				logger.Error("Unknown error", zap.Error(err))
			}
			break
		}
		idx := bytes.IndexByte(line, ' ')
		if idx == -1 {
			Metrics.ParseErrors.Add(1)
			logger.Error("Line doesn't contain any spaces", zap.String("line", string(line)))
			continue
		}

		// Compute the hash here, so workers will get the same metrics all the time
		hash.Write(line[:idx])
		pos := int(hash.Sum32() % uint32(Config.Senders))
		dataBuffer[pos] = append(dataBuffer[pos], line)
		metricsPending++

		// TODO: Rethink the limits here
		if metricsPending >= Config.ClickhouseSendInterval || reader.Buffered() == 0 {
			metricsPending = 0
			for cnt := range dataBuffer {
				queues[cnt].Lock()
				if len(queues[cnt].data) > Config.QueueLimitElements {
					l := int64(len(queues[cnt].data))
					queues[cnt].data = queues[cnt].data[l:]
					Metrics.MetricsDropped.Add(l)
					logger.Error("Queue size is too large, dropping oldest points...", zap.Int64("queue_size", l), zap.Int("queue_limit", Config.QueueLimitElements))
				}
				queues[cnt].data = append(queues[cnt].data, dataBuffer[cnt]...)
				queues[cnt].Unlock()
				dataBuffer[cnt] = dataBuffer[cnt][:0]
			}
		}
	}
}
