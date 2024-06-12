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
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	retry "github.com/snowplow-devops/go-retry"
	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/health"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/monitoring"
	"golang.org/x/oauth2"
)

type ResponseHandler struct {
	SuccessCriteria   []*Rule
	InvalidCriteria   []*Rule
	RetryableCriteria []*Rule
	RetryStrategies   map[string]RetryConfig
}

type Rule struct {
	HttpStatusExpectations []string
	*ResponseBodyExpectations

	//only for retries...
	RetryStrategy string
	Alert         string
}

type ResponseBodyExpectations struct {
	Path          string
	expectedValue string
}

type RetryConfig struct {
	Policy      string
	MaxAttempts int
	Delay       time.Duration
}

type Response struct {
	Status int
	Body   string
}

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

	//we have flat config structure everywhere so not sure if it's good idea to add struct here?
	ResponseHandler ResponseHandler
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

	//simply passing from HTTPTargetConfig
	responseHandler ResponseHandler
	monitoring      monitoring.Monitoring
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

func (ht *HTTPTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	ht.log.Debugf("Writing %d messages to endpoint ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		ht.MaximumAllowedMessageSizeBytes(),
	)

	var invalid []*models.Message
	var failed []*models.Message
	var sent []*models.Message
	var errResult error

	for _, msg := range safeMessages {
		request := ht.createHTTPRequest(msg)
		response := ht.executeHTTPRequest(request, msg)
		ht.handleHTTPResponse(request, response, msg, sent, invalid, failed, errResult, nil)
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error sending http requests")
	}

	ht.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		oversized,
		invalid,
	), errResult
}

func (ht *HTTPTarget) createHTTPRequest(msg *models.Message) *http.Request {
	//we have it defined in default config
	setupConfig := ht.responseHandler.RetryStrategies["setup"]

	// First place with possible failure - creating http request. The only way it can fail in our case is unparseable URL.
	// It's kind of setup error. So I imagine it would be nice to receive some alert about invalid URL?
	result := ht.retry(setupConfig, func() (interface{}, error) {
		request, err := http.NewRequest("POST", ht.httpURL, bytes.NewBuffer(msg.Data))

		if err != nil {
			ht.monitoring.SendAlert(monitoring.Alert{Message: fmt.Sprintf("Could not create HTTP request, error - %s", err.Error())})
			health.SetUnhealthy()
			return nil, err
		}

		request.Header.Add("Content-Type", ht.contentType)
		addHeadersToRequest(request, ht.headers, ht.retrieveHeaders(msg))
		if ht.basicAuthUsername != "" && ht.basicAuthPassword != "" {
			request.SetBasicAuth(ht.basicAuthUsername, ht.basicAuthPassword)
		}
		return nil, err
	})

	return result.(*http.Request)
}

func (ht *HTTPTarget) executeHTTPRequest(request *http.Request, msg *models.Message) Response {
	// Second possible place for failure - error after making HTTP call. We don't have HTTP response, so there is no way to check status/response body.
	// It's connection/response timeout or some other unexpected network issue.
	// Could we categorize it as transient error? So it deserves bunch of retries, after reaching max attempt just fail.
	transientConfig := ht.responseHandler.RetryStrategies["transient"]
	result := ht.retry(transientConfig, func() (interface{}, error) {
		requestStarted := time.Now()
		resp, err := ht.client.Do(request)
		requestFinished := time.Now()

		msg.TimeRequestStarted = requestStarted
		msg.TimeRequestFinished = requestFinished
		if err != nil {
			health.SetUnhealthy()
			return nil, err
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return &Response{Body: string(body), Status: resp.StatusCode}, nil
	})

	return result.(Response)
}

// We have HTTP response so we can check status and response body details.
// Returned error means we have to retry
func (ht *HTTPTarget) handleHTTPResponse(request *http.Request, response Response, msg *models.Message, sent []*models.Message, invalid []*models.Message, failed []*models.Message, errResult error, previouslyMatchedRule *Rule) error {
	//ACK and set healthy on success...
	if ht.isSuccess(response) {
		health.SetHealthy()
		sent = append(sent, msg)
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
		return nil
	}

	if ht.isInvalid(response) {
		// App is healthy, our data is not
		health.SetHealthy()
		invalid = append(invalid, msg)
		return nil
	}

	if currentRule := ht.findRetryableRule(response); currentRule != nil {
		//we found matching rule! Start retrying
		return ht.handleRetryableResponse(currentRule, response, previouslyMatchedRule, request, msg, sent, invalid, failed, errResult)
	}

	//no success/invalid/retryable rule matches, so we fallback to 'old default' layer of retrying (in main) and append to failed list.
	errResult = multierror.Append(errResult, errors.New("TODO"))
	failed = append(failed, msg)
	return nil
}

func (ht *HTTPTarget) handleRetryableResponse(currentRule *Rule, response Response, previouslyMatchedRule *Rule, request *http.Request, msg *models.Message, sent []*models.Message, invalid []*models.Message, failed []*models.Message, errResult error) error {
	health.SetUnhealthy()
	//send alert if configured...
	if currentRule.Alert != "" {
		ht.monitoring.SendAlert(monitoring.Alert{Message: fmt.Sprintf("%s, response body - %s", currentRule.Alert, response.Body)})
	}
	// If we have some 'previouslyMatchedRule' then we know we're currently retrying some request. It answers question - 'are we currently retrying something??'.
	// If it's equal to the new rule we've just matched, we have to retry again using the same strategy, instead of starting new retry loop. That's why we stop here and return control to the caller.
	if currentRule == previouslyMatchedRule {
		return errors.New("Same rule, probably same error, retry using the same strategy in upper layer")
	}

	// If `previouslyMatchedRule` is not defined (e.g. at the beginning it's ) or is not equal to the new rule (so we have some different error to deal with) we change retrying strategy.
	strategy := ht.responseHandler.RetryStrategies[currentRule.RetryStrategy]

	ht.retry(strategy, func() (interface{}, error) {
		response := ht.executeHTTPRequest(request, msg)
		return nil, ht.handleHTTPResponse(request, response, msg, sent, invalid, failed, errResult, currentRule)
	})
	return nil
}

func (ht *HTTPTarget) retry(config RetryConfig, action func() (interface{}, error)) interface{} {
	// Should we use some third-party retrying library? Where we can configure more stuff.
	// Now we're fixed on exponential.
	result, err := retry.ExponentialWithInterface(config.MaxAttempts, config.Delay, "HTTP target", action)
	// If we run out of attempts just crash?
	if err != nil {
		log.Fatal("Time to crash..?", err)
	}
	return result
}

func (ht *HTTPTarget) isSuccess(bodyWithStatus Response) bool {
	return findMatchingRule(bodyWithStatus, ht.responseHandler.SuccessCriteria) != nil
}

func (ht *HTTPTarget) isInvalid(bodyWithStatus Response) bool {
	return findMatchingRule(bodyWithStatus, ht.responseHandler.InvalidCriteria) != nil
}

func (ht *HTTPTarget) findRetryableRule(bodyWithStatus Response) *Rule {
	return findMatchingRule(bodyWithStatus, ht.responseHandler.RetryableCriteria)
}

func findMatchingRule(bodyWithStatus Response, rules []*Rule) *Rule {
	for _, rule := range rules {
		if ruleMatches(bodyWithStatus, rule) {
			return rule
		}
	}
	return nil
}

func ruleMatches(bodyWithStatus Response, rule *Rule) bool {
	codeMatch := httpStatusMatches(bodyWithStatus.Status, rule.HttpStatusExpectations)
	if rule.ResponseBodyExpectations != nil {
		return codeMatch && responseBodyMatches(bodyWithStatus.Body, rule.ResponseBodyExpectations)
	}
	return codeMatch
}

func httpStatusMatches(actual int, expectedPatterns []string) bool {
	return false
}

func responseBodyMatches(actual string, expectations *ResponseBodyExpectations) bool {
	return false
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

func (ht *HTTPTarget) retrieveHeaders(msg *models.Message) map[string]string {
	if !ht.dynamicHeaders {
		return nil
	}

	return msg.HTTPHeaders
}
