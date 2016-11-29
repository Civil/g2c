package main

import (
	"github.com/uber-go/zap"
	"io"
	"io/ioutil"
	"net/http"
)

func sendData(client *http.Client, endpoint string, buffer io.Reader) error {
	res, err := client.Post(endpoint, "text/plain", buffer)
	if err != nil {
		logger.Error("Error posting data to clickhouse", zap.Error(err))
	} else {
		if res.StatusCode != 200 {
			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				logger.Error("Clickhouse returned an error, can't read body", zap.Int("code", res.StatusCode))
			} else {
				logger.Error("Clickhouse returned not 200 OK", zap.Int("code", res.StatusCode), zap.String("body", string(body)))
			}
		} else {
			_, err = io.Copy(ioutil.Discard, res.Body)
			if err != nil {
				logger.Error("Error while discarding body", zap.Error(err))
			}
		}
		_ = res.Body.Close()
	}
	return err

}
