package rubber

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"sync/atomic"
	"time"

	"github.com/Sirupsen/logrus"
)

// Settings define how the single server client works
type Settings struct {
	// Simple settings
	Nodes     []string
	Preferred string

	// Weighted Settings
	Seed  string
	Limit int

	// Generic settings
	Timeout time.Duration
	Type    string
}

type singleServerBackend struct {
	log     *logrus.Logger
	nodes   []string
	timeout time.Duration

	client *http.Client

	retriesCounter uint64
}

func (es *singleServerBackend) CountRetries() uint64 { return atomic.SwapUint64(&es.retriesCounter, 0) }

func (es *singleServerBackend) Request(index, method, urlPath string, body io.Reader) (int, []byte, error) {
	var retries uint64

	ctxt := es.log.WithFields(logrus.Fields{
		"structure": "singleServerBackend",
		"function":  "Request",
		"method":    method,
	})

	for _, node := range es.nodes {
		url := fmt.Sprintf("http://%s%s", node, path.Join("/", index, urlPath))

		ctxt = ctxt.WithField("url", url)

		req, err := http.NewRequest(method, url, body)
		if err != nil {
			atomic.AddUint64(&es.retriesCounter, retries)
			return 0, []byte{}, err
		}

		startTime := time.Now()
		resp, err := es.client.Do(req)
		elapsedTime := time.Since(startTime)

		ctxt = ctxt.WithField("elapsed", elapsedTime.String())

		if err != nil {
			ctxt.WithField("error", err.Error()).Error("trying next node...")
			retries++
			continue
		}
		defer resp.Body.Close()

		ctxt = ctxt.WithFields(logrus.Fields{
			"elapsed":  elapsedTime.String(),
			"httpCode": resp.StatusCode,
		})

		reqResponse, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			atomic.AddUint64(&es.retriesCounter, retries)
			return 0, []byte{}, err
		}

		if resp.StatusCode < http.StatusInternalServerError {
			atomic.AddUint64(&es.retriesCounter, retries)
			return resp.StatusCode, reqResponse, nil
		}

		ctxt.Error(string(reqResponse))
		ctxt.Error("trying next node...")
		retries++
	}
	atomic.AddUint64(&es.retriesCounter, retries)
	return 0, []byte{}, errors.New("elasticsearch: request failed on all nodes")
}
