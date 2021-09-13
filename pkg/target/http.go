// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// HTTPTarget holds a new client for writing messages to HTTP endpoints
type HTTPTarget struct {
	client            *http.Client
	httpURL           string
	byteLimit         int
	contentType       string
	headers           map[string]string
	basicAuthUsername string
	basicAuthPassword string
	log               *log.Entry
}

func checkURL(str string) error {
	u, err := url.Parse(str)
	if err != nil {
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		return errors.New(fmt.Sprintf("Invalid url for Http target: '%s'", str))
	}
	return nil
}

// getHeaders expects a JSON object with key-value pairs, eg: `{"Max Forwards": "10", "Accept-Language": "en-US", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}`
func getHeaders(headers string) (map[string]string, error) {
	if headers == "" { // No headers is acceptable
		return nil, nil
	}
	var parsed map[string]string

	err := json.Unmarshal([]byte(headers), &parsed)
	if err != nil {
		return nil, errors.Wrap(err, "Error parsing headers. Ensure that headers are provided as a JSON of string key-value pairs")
	}

	return parsed, nil
}

func addHeadersToRequest(request *http.Request, headers map[string]string) {
	if headers == nil {
		return
	}

	for key, value := range headers {
		request.Header.Add(key, value)
	}

}

// NewHTTPTarget creates a client for writing events to HTTP
func NewHTTPTarget(httpURL string, requestTimeout int, byteLimit int, contentType string, headers string, basicAuthUsername string, basicAuthPassword string,
	certFile string, keyFile string, caFile string, skipVerifyTLS bool) (*HTTPTarget, error) {
	err := checkURL(httpURL)
	if err != nil {
		return nil, err
	}
	parsedHeaders, err1 := getHeaders(headers)
	if err1 != nil {
		return nil, err1
	}
	tlsConfig, err2 := CreateTLSConfiguration(certFile, keyFile, caFile, skipVerifyTLS)
	if err2 != nil {
		return nil, err2
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	return &HTTPTarget{
		client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(requestTimeout) * time.Second,
		},
		httpURL:           httpURL,
		byteLimit:         byteLimit,
		contentType:       contentType,
		headers:           parsedHeaders,
		basicAuthUsername: basicAuthUsername,
		basicAuthPassword: basicAuthPassword,
		log:               log.WithFields(log.Fields{"target": "http", "url": httpURL}),
	}, nil
}

func (ht *HTTPTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	ht.log.Debugf("Writing %d messages to topic ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		ht.MaximumAllowedMessageSizeBytes(),
	)

	var invalid []*models.Message
	var failed []*models.Message
	var sent []*models.Message
	var errResult error

	for _, msg := range safeMessages {

		request, err := http.NewRequest("POST", ht.httpURL, bytes.NewBuffer(msg.Data))
		if err != nil {
			errResult = multierror.Append(errResult, errors.Wrap(err, "Error creating request"))
			failed = append(failed, msg)
			continue
		}
		request.Header.Add("Content-Type", ht.contentType)            // Add content type
		addHeadersToRequest(request, ht.headers)                      // Add headers if there are any
		if ht.basicAuthUsername != "" && ht.basicAuthPassword != "" { // Add basic auth if set
			request.SetBasicAuth(ht.basicAuthUsername, ht.basicAuthPassword)
		}
		resp, err := ht.client.Do(request) // Make request
		if err != nil {
			errResult = multierror.Append(errResult, err)
			failed = append(failed, msg)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			sent = append(sent, msg)
			if msg.AckFunc != nil { // Ack successful messages
				msg.AckFunc()
			}
		} else {
			body, _ := ioutil.ReadAll(resp.Body) // Any error reading the body of the response is ignored, since the important thing is to surface the failing status.
			errResult = multierror.Append(errResult, errors.Wrap(errors.New(string(body)), resp.Status))
			failed = append(failed, msg)
			continue
		}
	}
	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error sending http request")
	}

	ht.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		invalid,
	), errResult
}

// Open does nothing for this target
func (ht *HTTPTarget) Open() {}

// Close does nothing for this target
func (ht *HTTPTarget) Close() {}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (ht *HTTPTarget) MaximumAllowedMessageSizeBytes() int {
	return ht.byteLimit
}

// GetID returns an identifier for this target
func (ht *HTTPTarget) GetID() string {
	return ht.httpURL
}
