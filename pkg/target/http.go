/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package target

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/batchtransform"
	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"

	"golang.org/x/oauth2"
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
	DynamicHeaders          bool   `hcl:"dynamic_headers,optional" env:"TARGET_HTTP_DYNAMIC_HEADERS"`

	OAuth2ClientID     string `hcl:"oauth2_client_id,optional" env:"TARGET_HTTP_OAUTH2_CLIENT_ID"`
	OAuth2ClientSecret string `hcl:"oauth2_client_secret,optional" env:"TARGET_HTTP_OAUTH2_CLIENT_SECRET"`
	OAuth2RefreshToken string `hcl:"oauth2_refresh_token,optional" env:"TARGET_HTTP_OAUTH2_REFRESH_TOKEN"`
	OAuth2TokenURL     string `hcl:"oauth2_token_url,optional" env:"TARGET_HTTP_OAUTH2_TOKEN_URL"`
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
	dynamicHeaders    bool
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

func addHeadersToRequest(request *http.Request, headers map[string]string, dynamicHeaders map[string]string) {
	for key, value := range headers {
		request.Header.Add(key, value)
	}

	for key, value := range dynamicHeaders {
		request.Header.Add(key, value)
	}
}

// newHTTPTarget creates a client for writing events to HTTP
func newHTTPTarget(httpURL string, requestTimeout int, byteLimit int, contentType string, headers string, basicAuthUsername string, basicAuthPassword string,
	certFile string, keyFile string, caFile string, skipVerifyTLS bool, dynamicHeaders bool, oAuth2ClientID string, oAuth2ClientSecret string, oAuth2RefreshToken string, oAuth2TokenURL string) (*HTTPTarget, error) {
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

	client := createHTTPClient(oAuth2ClientID, oAuth2ClientSecret, oAuth2TokenURL, oAuth2RefreshToken, transport)
	client.Timeout = time.Duration(requestTimeout) * time.Second

	return &HTTPTarget{
		client:            client,
		httpURL:           httpURL,
		byteLimit:         byteLimit,
		contentType:       contentType,
		headers:           parsedHeaders,
		basicAuthUsername: basicAuthUsername,
		basicAuthPassword: basicAuthPassword,
		log:               log.WithFields(log.Fields{"target": "http", "url": httpURL}),
		dynamicHeaders:    dynamicHeaders,
	}, nil
}

func createHTTPClient(oAuth2ClientID string, oAuth2ClientSecret string, oAuth2TokenURL string, oAuth2RefreshToken string, transport *http.Transport) *http.Client {
	if oAuth2ClientID != "" {
		oauth2Config := oauth2.Config{
			ClientID:     oAuth2ClientID,
			ClientSecret: oAuth2ClientSecret,
			Endpoint: oauth2.Endpoint{
				TokenURL: oAuth2TokenURL,
			},
		}

		token := &oauth2.Token{RefreshToken: oAuth2RefreshToken}
		return oauth2Config.Client(context.Background(), token)
	}

	return &http.Client{
		Transport: transport,
	}
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
		c.DynamicHeaders,
		c.OAuth2ClientID,
		c.OAuth2ClientSecret,
		c.OAuth2RefreshToken,
		c.OAuth2TokenURL,
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

// When we have dynamic headers, batching by header must necessarily run first. This is a http specific function,
// so defining it here and fixing it into the Write function avoids complexity in configuration
func (ht *HTTPTarget) groupByDynamicHeaders(batches []models.MessageBatch) ([]models.MessageBatch, []*models.Message, []*models.Message) {
	if !ht.dynamicHeaders {
		// If the feature is disabled just return
		return batches, nil, nil
	}

	// Make a map of stringified header values
	headersFound := make(map[string]*models.MessageBatch)

	for _, batch := range batches {
		// Group data by that index
		for _, msg := range batch.OriginalMessages {
			headerKey := fmt.Sprint(msg.HTTPHeaders)
			if headersFound[headerKey] != nil {
				// If a key already exists, just add this message
				headersFound[headerKey].OriginalMessages = append(headersFound[headerKey].OriginalMessages, msg)
			} else {
				headersFound[headerKey] = &models.MessageBatch{
					OriginalMessages: []*models.Message{msg},
					HTTPHeaders:      ht.retrieveHeaders(msg),
				}
			}
		}
	}

	outBatches := []models.MessageBatch{}
	for _, batch := range headersFound {
		outBatches = append(outBatches, *batch)
	}

	return outBatches, nil, nil
}

// Where no transformation function provides a request body, we must provide one - this necessarily must happen last.
// This is a http specific function so we define it here to avoid scope for misconfiguration
func (ht *HTTPTarget) provideRequestBody(batches []models.MessageBatch) ([]models.MessageBatch, []*models.Message, []*models.Message) {

	// TODO: Add test for when messagess are just strings & confirm that it all works

	// TODO: Note: This would mean that the GTM client gets arrays of single events instead of single events.
	// But we could configure an explicit templater to change that if we wanted
	// We should test to be certain that it's still compatible.
	invalid := make([]*models.Message, 0)
	for _, batch := range batches {
		if batch.BatchData != nil {
			// TODO: Check if it is nil or zero & adapt accordingly
			continue
		}
		requestData := []string{}
		for _, msg := range batch.OriginalMessages {
			requestData = append(requestData, string(msg.Data))
		}
		// TODO: Add tests to be sure this produces the desired request
		requestBody, err := json.Marshal(requestData)
		if err != nil {
			// TODO: Handle errors here
			fmt.Println(err)
		}
		batch.BatchData = requestBody
	}

	return batches, invalid, nil
}

func (ht *HTTPTarget) Write(messages []*models.Message, batchTransformFunc batchtransform.BatchTransformationApplyFunction) (*models.TargetWriteResult, error) {
	ht.log.Debugf("Writing %d messages to endpoint ...", len(messages))

	// TODO: We are changing this target from always operating on single events to now handling batches
	// this should therefore be replaced by the new chunking Batch Transformation.
	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		ht.MaximumAllowedMessageSizeBytes(),
	)

	var invalid []*models.Message
	var failed []*models.Message
	var sent []*models.Message
	var errResult error

	// Run the transformations
	// We provide a 'pre' function to group by Dynamic headers (if enabled) - this must necessarily happen first.
	// We also provide a 'post' function to create a message Body if none is provided via templater - this must happen last.
	batchTransformRes := batchTransformFunc(safeMessages, []batchtransform.BatchTransformationFunction{ht.groupByDynamicHeaders}, []batchtransform.BatchTransformationFunction{ht.provideRequestBody})

	invalid = append(invalid, batchTransformRes.Invalid...)

	for _, batch := range batchTransformRes.Success {
		request, err := http.NewRequest("POST", ht.httpURL, bytes.NewBuffer(batch.BatchData))
		if err != nil {
			errResult = multierror.Append(errResult, errors.Wrap(err, "Error creating request"))
			failed = append(failed, batch.OriginalMessages...)
			continue
		}
		request.Header.Add("Content-Type", ht.contentType)            // Add content type
		addHeadersToRequest(request, ht.headers, batch.HTTPHeaders)   // Add headers
		if ht.basicAuthUsername != "" && ht.basicAuthPassword != "" { // Add basic auth if set
			request.SetBasicAuth(ht.basicAuthUsername, ht.basicAuthPassword)
		}
		requestStarted := time.Now()
		resp, err := ht.client.Do(request) // Make request
		requestFinished := time.Now()

		if err != nil {
			errResult = multierror.Append(errResult, err)
			// TODO: This means that errored requests won't have request times attached.
			// Can iterate to add or rethink how it works.
			failed = append(failed, batch.OriginalMessages...)
			continue
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			for _, msg := range batch.OriginalMessages {
				msg.TimeRequestStarted = requestStarted
				msg.TimeRequestFinished = requestFinished
				sent = append(sent, msg)
				if msg.AckFunc != nil { // Ack successful messages
					msg.AckFunc()
				}
			}

		} else {
			errResult = multierror.Append(errResult, errors.New("Got response status: "+resp.Status))
			failed = append(failed, batch.OriginalMessages...)
			continue
		}
	}
	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error sending http request(s)")
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

// TODO: Can prob remove
func (ht *HTTPTarget) retrieveHeaders(msg *models.Message) map[string]string {
	if !ht.dynamicHeaders {
		return nil
	}

	return msg.HTTPHeaders
}
