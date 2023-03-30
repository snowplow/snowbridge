//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package target

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"
)

// HTTPTargetConfig configures the destination for records consumed
type HTTPTargetConfig struct {
	HTTPURL                 string `hcl:"url" env:"TARGET_HTTP_URL"`
	ByteLimit               int    `hcl:"byte_limit,optional" env:"TARGET_HTTP_BYTE_LIMIT"`
	RequestTimeoutInSeconds int    `hcl:"request_timeout_in_seconds,optional" env:"TARGET_HTTP_TIMEOUT_IN_SECONDS"`
	ContentType             string `hcl:"content_type,optional" env:"TARGET_HTTP_CONTENT_TYPE"`
	Headers                 string `hcl:"headers,optional" env:"TARGET_HTTP_HEADERS" `
	BasicAuthUsername       string `hcl:"basic_auth_username,optional" env:"TARGET_HTTP_BASICAUTH_USERNAME"`
	BasicAuthPassword       string `hcl:"basic_auth_password,optional" env:"TARGET_HTTP_BASICAUTH_PASSWORD"`
	CertFile                string `hcl:"cert_file,optional" env:"TARGET_HTTP_TLS_CERT_FILE"`
	KeyFile                 string `hcl:"key_file,optional" env:"TARGET_HTTP_TLS_KEY_FILE"`
	CaFile                  string `hcl:"ca_file,optional" env:"TARGET_HTTP_TLS_CA_FILE"`
	SkipVerifyTLS           bool   `hcl:"skip_verify_tls,optional" env:"TARGET_HTTP_TLS_SKIP_VERIFY_TLS"` // false
}

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
		return errors.New(fmt.Sprintf("Invalid url for HTTP target: '%s'", str))
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

// newHTTPTarget creates a client for writing events to HTTP
func newHTTPTarget(httpURL string, requestTimeout int, byteLimit int, contentType string, headers string, basicAuthUsername string, basicAuthPassword string,
	certFile string, keyFile string, caFile string, skipVerifyTLS bool) (*HTTPTarget, error) {
	err := checkURL(httpURL)
	if err != nil {
		return nil, err
	}
	parsedHeaders, err1 := getHeaders(headers)
	if err1 != nil {
		return nil, err1
	}
	transport := &http.Transport{}

	tlsConfig, err2 := common.CreateTLSConfiguration(certFile, keyFile, caFile, skipVerifyTLS)
	if err2 != nil {
		return nil, err2
	}
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}

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

// HTTPTargetConfigFunction creates HTTPTarget from HTTPTargetConfig
func HTTPTargetConfigFunction(c *HTTPTargetConfig) (*HTTPTarget, error) {
	return newHTTPTarget(
		c.HTTPURL,
		c.RequestTimeoutInSeconds,
		c.ByteLimit,
		c.ContentType,
		c.Headers,
		c.BasicAuthUsername,
		c.BasicAuthPassword,
		c.CertFile,
		c.KeyFile,
		c.CaFile,
		c.SkipVerifyTLS,
	)
}

// The HTTPTargetAdapter type is an adapter for functions to be used as
// pluggable components for HTTP Target. It implements the Pluggable interface.
type HTTPTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f HTTPTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f HTTPTargetAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &HTTPTargetConfig{
		ByteLimit:               1048576,
		RequestTimeoutInSeconds: 5,
		ContentType:             "application/json",
	}

	return cfg, nil
}

// AdaptHTTPTargetFunc returns an HTTPTargetAdapter.
func AdaptHTTPTargetFunc(f func(c *HTTPTargetConfig) (*HTTPTarget, error)) HTTPTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*HTTPTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected HTTPTargetConfig")
		}

		return f(cfg)
	}
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
		requestStarted := time.Now()
		resp, err := ht.client.Do(request) // Make request
		requestFinished := time.Now()

		msg.TimeRequestStarted = requestStarted
		msg.TimeRequestFinished = requestFinished

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
			errResult = multierror.Append(errResult, errors.New("Got response status: "+resp.Status))
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
