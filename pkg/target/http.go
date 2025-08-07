/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
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
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"

	"golang.org/x/oauth2"
)

const SupportedTargetHTTP = "http"

// HTTPTargetConfig configures the destination for records consumed
type HTTPTargetConfig struct {
	URL                     string `hcl:"url"`
	RequestTimeoutInSeconds int    `hcl:"request_timeout_in_seconds,optional"`
	RequestTimeoutInMillis  int    `hcl:"request_timeout_in_millis,optional"`
	ContentType             string `hcl:"content_type,optional"`
	Headers                 string `hcl:"headers,optional"`
	BasicAuthUsername       string `hcl:"basic_auth_username,optional"`
	BasicAuthPassword       string `hcl:"basic_auth_password,optional"`

	EnableTLS      bool   `hcl:"enable_tls,optional"`
	CertFile       string `hcl:"cert_file,optional"`
	KeyFile        string `hcl:"key_file,optional"`
	CaFile         string `hcl:"ca_file,optional"`
	SkipVerifyTLS  bool   `hcl:"skip_verify_tls,optional"` // false
	DynamicHeaders bool   `hcl:"dynamic_headers,optional"`

	OAuth2ClientID     string `hcl:"oauth2_client_id,optional"`
	OAuth2ClientSecret string `hcl:"oauth2_client_secret,optional"`
	OAuth2RefreshToken string `hcl:"oauth2_refresh_token,optional"`
	OAuth2TokenURL     string `hcl:"oauth2_token_url,optional"`

	RequestMaxMessages int `hcl:"request_max_messages,optional"`
	RequestByteLimit   int `hcl:"request_byte_limit,optional"` // note: breaking change here
	MessageByteLimit   int `hcl:"message_byte_limit,optional"`

	TemplateFile  string         `hcl:"template_file,optional"`
	ResponseRules *ResponseRules `hcl:"response_rules,block"`

	IncludeTimingHeaders       bool `hcl:"include_timing_headers,optional"`
	RejectionThresholdInMillis int  `hcl:"rejection_threshold_in_millis,optional"`
}

// ResponseRules is part of HTTP target configuration. It provides rules how HTTP respones should be handled. Response can be categerized as 'invalid' (bad data), as setup error or (if none of the rules matches) as a transient error.
type ResponseRules struct {
	Invalid    []Rule `hcl:"invalid,block"`
	SetupError []Rule `hcl:"setup,block"`
}

// Rule configuration defines what kind of values are expected to exist in HTTP response, like status code or message in the body.
type Rule struct {
	MatchingHTTPCodes []int  `hcl:"http_codes,optional"`
	MatchingBodyPart  string `hcl:"body,optional"`
}

// Helper struct storing response HTTP status code and parsed response body
type response struct {
	Status int
	Body   string
}

// HTTPTarget holds a new client for writing messages to HTTP endpoints
type HTTPTarget struct {
	client            *http.Client
	httpURL           string
	contentType       string
	headers           map[string]string
	basicAuthUsername string
	basicAuthPassword string
	log               *log.Entry
	dynamicHeaders    bool

	requestMaxMessages int
	requestByteLimit   int
	messageByteLimit   int

	requestTemplate *template.Template
	approxTmplSize  int
	responseRules   *ResponseRules

	includeTimingHeaders bool
	rejectionThreshold   int
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

func loadRequestTemplate(templateFile string) (int, *template.Template, error) {
	if templateFile != "" {
		content, err := os.ReadFile(templateFile)

		if err != nil {
			return 0, nil, err
		}
		tmpl, err := parseRequestTemplate(string(content))
		return len(content), tmpl, err
	}
	return 0, nil, nil
}

func parseRequestTemplate(templateContent string) (*template.Template, error) {
	customTemplateFunctions := template.FuncMap{
		// If you use this in your template on struct-like fields, you get rendered nice JSON `{"field":"value"}` instead of stringified map `map[field:value]`
		"prettyPrint": func(v any) (string, error) {
			bytes, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return string(bytes), nil
		},
		"env": func(name string) string {
			return os.Getenv(name)
		},
	}

	parsedTemplate, err := template.New("HTTP").Funcs(customTemplateFunctions).Parse(templateContent)
	if err != nil {
		return nil, err
	}

	return parsedTemplate, nil
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
	var requestTimeoutInMillis int

	if c.RequestTimeoutInMillis != 0 && c.RequestTimeoutInSeconds == 0 {
		requestTimeoutInMillis = c.RequestTimeoutInMillis
	}

	if c.RequestTimeoutInMillis != 0 && c.RequestTimeoutInSeconds != 0 {
		requestTimeoutInMillis = c.RequestTimeoutInMillis
		log.Warn("Both 'request_timeout_in_millis' and 'request_timeout_in_seconds' options are set. In this case 'request_timeout_in_millis' takes precendence and 'request_timeout_in_seconds' is ignored. Using 'request_timeout_in_seconds' is deprecated, and will be removed in the next major version. Use 'request_timeout_in_millis' only")
	}

	if c.RequestTimeoutInMillis == 0 && c.RequestTimeoutInSeconds != 0 {
		requestTimeoutInMillis = c.RequestTimeoutInSeconds * 1000
		log.Warn("For the HTTP target, 'request_timeout_in_seconds' is deprecated, and will be removed in the next major version. Use 'request_timeout_in_millis' instead")
	}

	if c.RequestTimeoutInMillis == 0 && c.RequestTimeoutInSeconds == 0 {
		requestTimeoutInMillis = 5000
		log.Warn("Neither 'request_timeout_in_millis' nor 'request_timeout_in_seconds' are set. The previous default is preserved, but strongly advise manual configuration of 'request_timeout_in_millis'")
	}

	err := common.CheckURL(c.URL)
	if err != nil {
		return nil, err
	}
	parsedHeaders, err1 := getHeaders(c.Headers)
	if err1 != nil {
		return nil, err1
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns

	tlsConfig, err2 := common.CreateTLSConfiguration(c.CertFile, c.KeyFile, c.CaFile, c.SkipVerifyTLS)
	if err2 != nil {
		return nil, err2
	}

	if c.EnableTLS && tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}

	client := createHTTPClient(c.OAuth2ClientID, c.OAuth2ClientSecret, c.OAuth2TokenURL, c.OAuth2RefreshToken, transport)
	client.Timeout = time.Duration(requestTimeoutInMillis) * time.Millisecond

	approxTmplSize, requestTemplate, err := loadRequestTemplate(c.TemplateFile)
	if err != nil {
		return nil, err
	}
	if approxTmplSize >= c.RequestByteLimit || approxTmplSize >= c.MessageByteLimit {
		return nil, errors.New("target error: Byte limit must be larger than template size")
	}

	return &HTTPTarget{
		client:               client,
		httpURL:              c.URL,
		contentType:          c.ContentType,
		headers:              parsedHeaders,
		basicAuthUsername:    c.BasicAuthUsername,
		basicAuthPassword:    c.BasicAuthPassword,
		log:                  log.WithFields(log.Fields{"target": SupportedTargetHTTP, "url": c.URL}),
		dynamicHeaders:       c.DynamicHeaders,
		requestMaxMessages:   c.RequestMaxMessages,
		requestByteLimit:     c.RequestByteLimit,
		messageByteLimit:     c.MessageByteLimit,
		requestTemplate:      requestTemplate,
		approxTmplSize:       approxTmplSize,
		responseRules:        c.ResponseRules,
		includeTimingHeaders: c.IncludeTimingHeaders,
		rejectionThreshold:   c.RejectionThresholdInMillis,
	}, nil
}

// The HTTPTargetAdapter type is an adapter for functions to be used as
// pluggable components for HTTP Target. It implements the Pluggable interface.
type HTTPTargetAdapter func(i any) (any, error)

// Create implements the ComponentCreator interface.
func (f HTTPTargetAdapter) Create(i any) (any, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f HTTPTargetAdapter) ProvideDefault() (any, error) {
	return defaultConfiguration(), nil
}

func defaultConfiguration() *HTTPTargetConfig {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &HTTPTargetConfig{
		RequestMaxMessages: 20,
		RequestByteLimit:   1048576,
		MessageByteLimit:   1048576,
		EnableTLS:          false,

		ContentType: "application/json",
		ResponseRules: &ResponseRules{
			Invalid:    []Rule{},
			SetupError: []Rule{},
		},
		IncludeTimingHeaders:       false,
		RejectionThresholdInMillis: 150,
	}

	return cfg
}

// AdaptHTTPTargetFunc returns an HTTPTargetAdapter.
func AdaptHTTPTargetFunc(f func(c *HTTPTargetConfig) (*HTTPTarget, error)) HTTPTargetAdapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*HTTPTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected HTTPTargetConfig")
		}

		return f(cfg)
	}
}

func (ht *HTTPTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	ht.log.Debugf("Writing %d messages to endpoint ...", len(messages))

	chunks, oversized := models.GetChunkedMessages(
		messages,
		ht.requestMaxMessages,
		ht.messageByteLimit-ht.approxTmplSize,
		ht.requestByteLimit-ht.approxTmplSize,
	)

	sent := []*models.Message{}
	failed := []*models.Message{}
	invalid := []*models.Message{}
	var errResult error
	var hitSetupError bool

	for _, chunk := range chunks {
		grouped := ht.groupByDynamicHeaders(chunk)

		for _, group := range grouped {
			var reqBody []byte
			var goodMsgs []*models.Message
			var badMsgs []*models.Message
			var err error

			if ht.requestTemplate != nil {
				reqBody, goodMsgs, badMsgs = ht.renderBatchUsingTemplate(group)
			} else {
				reqBody, goodMsgs, badMsgs = ht.renderJSONArray(group)
			}

			invalid = append(invalid, badMsgs...)

			if len(goodMsgs) == 0 {
				continue
			}

			request, err := http.NewRequest("POST", ht.httpURL, bytes.NewBuffer(reqBody))

			if err != nil {
				panic(err)
			}

			request.Header.Add("Content-Type", ht.contentType)                        // Add content type
			addHeadersToRequest(request, ht.headers, ht.retrieveHeaders(goodMsgs[0])) // Add headers if there are any - because they're grouped by header, we just need to pick the header from one message
			if ht.basicAuthUsername != "" && ht.basicAuthPassword != "" {             // Add basic auth if set
				request.SetBasicAuth(ht.basicAuthUsername, ht.basicAuthPassword)
			}

			requestStarted := time.Now().UTC()
			if ht.includeTimingHeaders {
				rejectionTimestamp := requestStarted.UnixMilli() + (ht.client.Timeout.Milliseconds() - int64(ht.rejectionThreshold))
				request.Header.Add("Rejection-Timestamp", strconv.FormatInt(rejectionTimestamp, 10))
			}

			resp, err := ht.client.Do(request) // Make request
			requestFinished := time.Now().UTC()

			// Add request times to every message
			for _, msg := range goodMsgs {
				msg.TimeRequestStarted = requestStarted
				msg.TimeRequestFinished = requestFinished
			}

			if err != nil {
				failed = append(failed, goodMsgs...)
				errResult = multierror.Append(errResult, errors.New("Error sending request: "+err.Error()))
				continue
			}

			defer func() {
				if _, err := io.Copy(io.Discard, resp.Body); err != nil {
					ht.log.Error(err.Error())
				}
				if err := resp.Body.Close(); err != nil {
					ht.log.Error(err.Error())
				}
			}()

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				for _, msg := range goodMsgs {
					if msg.AckFunc != nil { // Ack successful messages
						msg.AckFunc()
					}
					sent = append(sent, msg)
				}
				continue
			}

			// Process non-2xx responses
			responseBody, err := io.ReadAll(resp.Body)

			if err != nil {
				failed = append(failed, goodMsgs...)
				errResult = multierror.Append(errResult, errors.New("Error reading response body: "+err.Error()))
				continue
			}

			response := response{Body: string(responseBody), Status: resp.StatusCode}

			// Set errors with code and body for metadata reporting
			for _, msg := range goodMsgs {
				msg.SetError(&models.ApiError{
					StatusCode:   resp.Status,
					ResponseBody: response.Body,
					SafeMessage:  "Transient error",
				})
			}

			if matchedRule := findMatchingRule(response, ht.responseRules.Invalid); matchedRule != nil {
				for _, msg := range goodMsgs {
					msg.SetError(&models.ApiError{
						StatusCode:   resp.Status,
						ResponseBody: response.Body,
						SafeMessage:  "Invalid error",
					})
				}

				invalid = append(invalid, goodMsgs...)
				continue
			}

			var errorDetails error
			if rule := findMatchingRule(response, ht.responseRules.SetupError); rule != nil {
				hitSetupError = true

				if rule.MatchingBodyPart != "" {
					errorDetails = fmt.Errorf("got setup error, response status: '%s' with error details: '%s'", resp.Status, rule.MatchingBodyPart)
				} else {
					errorDetails = fmt.Errorf("got setup error, response status: '%s'", resp.Status)
				}

				for _, msg := range goodMsgs {
					msg.SetError(&models.ApiError{
						StatusCode:   resp.Status,
						ResponseBody: response.Body,
						SafeMessage:  "Setup error",
					})
				}

			} else {
				errorDetails = fmt.Errorf("got transient error, response status: '%s'", resp.Status)
			}
			errResult = multierror.Append(errResult, errorDetails)
			failed = append(failed, goodMsgs...)
		}
	}

	if hitSetupError {
		errResult = models.SetupWriteError{Err: errResult}
	}

	ht.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return models.NewTargetWriteResult(sent, failed, oversized, invalid), errResult
}

func findMatchingRule(res response, rules []Rule) *Rule {
	for _, rule := range rules {
		if ruleMatches(res, rule) {
			return &rule
		}
	}
	return nil
}

func ruleMatches(res response, rule Rule) bool {
	codeMatch := httpStatusMatches(res.Status, rule.MatchingHTTPCodes)
	if rule.MatchingBodyPart != "" {
		return codeMatch && responseBodyMatches(res.Body, rule.MatchingBodyPart)
	}
	return codeMatch
}

func httpStatusMatches(actual int, expectedCodes []int) bool {
	for _, expected := range expectedCodes {
		if expected == actual {
			return true
		}
	}
	return false
}

func responseBodyMatches(actual string, bodyPattern string) bool {
	return strings.Contains(actual, bodyPattern)
}

// Open does nothing for this target
func (ht *HTTPTarget) Open() {}

// Close does nothing for this target
func (ht *HTTPTarget) Close() {}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (ht *HTTPTarget) MaximumAllowedMessageSizeBytes() int {
	return ht.messageByteLimit
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

// renderBatchUsingTemplate creates a request from a batch of messages based on configured template
func (ht *HTTPTarget) renderBatchUsingTemplate(messages []*models.Message) (templated []byte, success []*models.Message, invalid []*models.Message) {
	validJsons := []any{}

	for _, msg := range messages {
		var asJSON any

		if err := json.Unmarshal(msg.Data, &asJSON); err != nil {
			msg.SetError(&models.TemplatingError{
				SafeMessage: "Message can't be parsed as valid JSON",
				Err:         err,
			})
			invalid = append(invalid, msg)
			continue
		}

		success = append(success, msg)
		validJsons = append(validJsons, asJSON)
	}

	var buf bytes.Buffer
	tmplErr := ht.requestTemplate.Execute(&buf, validJsons)
	if tmplErr != nil {
		for _, msg := range success {
			msg.SetError(&models.TemplatingError{
				SafeMessage: "Could not create request JSON",
				Err:         tmplErr,
			})
			invalid = append(invalid, msg)
		}
		return nil, nil, invalid
	}

	return buf.Bytes(), success, invalid
}

// Where no transformation function provides a request body, we must provide one - this necessarily must happen last.
// This is a http specific function so we define it here to avoid scope for misconfiguration
func (ht *HTTPTarget) renderJSONArray(messages []*models.Message) (templated []byte, success []*models.Message, invalid []*models.Message) {

	// This assumes the data is a valid JSON. Plain strings are no longer supported, but can be handled via a combination of transformation and templater
	requestData := make([]json.RawMessage, 0)
	for _, msg := range messages {
		var asRaw json.RawMessage
		// If any data is not json compatible, we must treat as invalid
		if err := json.Unmarshal(msg.Data, &asRaw); err != nil {
			msg.SetError(&models.TemplatingError{
				SafeMessage: "Message can't be parsed as valid JSON",
				Err:         err,
			})
			invalid = append(invalid, msg)
			continue
		}

		requestData = append(requestData, msg.Data)
		success = append(success, msg)
	}

	requestBody, err := json.Marshal(requestData)
	if err != nil {
		for _, msg := range success {
			msg.SetError(&models.TemplatingError{
				SafeMessage: "Could not create request JSON",
				Err:         err,
			})
			invalid = append(invalid, msg)
		}
		return nil, nil, invalid
	}

	return requestBody, success, invalid
}

// groupByDynamicHeaders batches data by header if the dynamic header feature is turned on.
func (ht *HTTPTarget) groupByDynamicHeaders(messages []*models.Message) [][]*models.Message {
	if !ht.dynamicHeaders {
		// If the feature is disabled just return
		return [][]*models.Message{messages}
	}

	// Make a map of stringified header values
	headersFound := make(map[string][]*models.Message)

	// Group data by that index
	for _, msg := range messages {
		headerKey := fmt.Sprint(msg.HTTPHeaders)
		if headersFound[headerKey] != nil {
			// If a key already exists, just add this message
			headersFound[headerKey] = append(headersFound[headerKey], msg)
		} else {
			headersFound[headerKey] = []*models.Message{msg}
		}
	}

	outBatches := [][]*models.Message{}
	for _, batch := range headersFound {
		outBatches = append(outBatches, batch)
	}

	return outBatches
}
