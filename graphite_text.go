package main

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/uber-go/zap"
	"hash/fnv"
	"io"
	"net"
	"strconv"
	"time"
)

// TODO: Make a module out of this code

var errParseTimestamp = errors.New("Can't parse timestamp")
var errParseGeneral = errors.New("Can't parse line")

func checkFloat(value []byte) error {
	_, err := strconv.ParseFloat(unsafeString(value), 64)
	return err
}

func checkTimestamp(value []byte) error {
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return errParseTimestamp
		}
	}
	return nil
}

// We don't need to validate point at this moment, DB will handle it better
func sanitizePoint(line []byte, number int) ([]byte, error) {
	spacesCnt := 0
	prevIdx := 0
	// TODO: use bytes.Index()
	for idx := range line {
		if line[idx] == ' ' {
			line[idx] = '\t'
			if spacesCnt == 0 {
			}
			if spacesCnt == 2 {
				logger.Error("more spaces than we expect", zap.Int("spaces", spacesCnt))
				return []byte{}, errParseGeneral
			}
			if spacesCnt == 1 {
				tmp := line[prevIdx:idx]
				err := checkFloat(tmp)
				if err != nil {
					logger.Error("Can't parse float value")
					return []byte{}, err
				}
			}
			if spacesCnt == 2 {
			}
			prevIdx = idx + 1
			spacesCnt++
		}
	}

	if spacesCnt != 2 {
		logger.Error("Found != 2 spaces", zap.Int("spaces", spacesCnt))
		return []byte{}, errParseGeneral
	}

	tmp := line[prevIdx : len(line)-2]
	err := checkTimestamp(tmp)
	if err != nil {
		logger.Error("Can't parse timestamp")
		return []byte{}, err
	}

	line[len(line)-1] = '\t'
	return line, nil
}

func preparePointText(line []byte, buffer *bytes.Buffer, date, version []byte, number int) error {
	point, err := sanitizePoint(line, number)
	if err != nil {
		return err
	}

	buffer.Write(point)
	buffer.Write(date)
	buffer.WriteByte('\t')
	buffer.Write(version)
	buffer.WriteByte('\n')
	return nil

}

func processGraphiteText(c net.Conn) {
	defer func() {
		err := c.Close()
		if err != nil {
			logger.Error("Can't close connection", zap.Error(err))
		}
	}()

	reader := bufio.NewReaderSize(c, 32*1024)
	metricsPending := 0
	lastDeadline := time.Now()
	readTimeout := 30 * time.Second
	err := c.SetReadDeadline(lastDeadline.Add(readTimeout))
	if err != nil {
		logger.Error("Failed to set read deadline", zap.Error(err))
		return
	}
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
		now := time.Now()
		if now.Sub(lastDeadline) > (readTimeout >> 2) {
			err = c.SetReadDeadline(now.Add(readTimeout))
			if err != nil {
				logger.Error("Failed to set read deadline", zap.Error(err))
				break
			}
			lastDeadline = now
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
