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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"

	"golang.org/x/oauth2"
)

const SupportedTargetHTTP = "http"

// HTTPTargetConfig configures the destination for records consumed
type HTTPTargetConfig struct {
	BatchingConfig *targetiface.BatchingConfig `hcl:"batching,block"`

	URL                    string            `hcl:"url"`
	RequestTimeoutInMillis int               `hcl:"request_timeout_in_millis,optional"`
	ContentType            string            `hcl:"content_type,optional"`
	Headers                map[string]string `hcl:"headers,optional"`
	BasicAuthUsername      string            `hcl:"basic_auth_username,optional"`
	BasicAuthPassword      string            `hcl:"basic_auth_password,optional"`

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

	TemplateFile     string         `hcl:"template_file,optional"`
	ResponseRules    *ResponseRules `hcl:"response_rules,block"`
	MetadataSafeMode bool           `hcl:"metadata_safe_mode,optional"`

	IncludeTimingHeaders       bool `hcl:"include_timing_headers,optional"`
	RejectionThresholdInMillis int  `hcl:"rejection_threshold_in_millis,optional"`
}

// ResponseRules is part of HTTP target configuration. It provides rules how HTTP responses should be handled. Response can be categorized as 'invalid' (bad data), as setup error or (if none of the rules matches) as a transient error.
type ResponseRules struct {
	Rules []Rule `hcl:"rule,block"`
}

type ResponseRuleType string

const (
	ResponseRuleTypeInvalid  ResponseRuleType = "invalid"
	ResponseRuleTypeSetup    ResponseRuleType = "setup"
	ResponseRuleTypeThrottle ResponseRuleType = "throttle"
	ResponseRuleTypeFatal    ResponseRuleType = "fatal"
)

func isValidResponseRuleType(ruleType ResponseRuleType) bool {
	switch ruleType {
	case ResponseRuleTypeInvalid, ResponseRuleTypeSetup, ResponseRuleTypeThrottle, ResponseRuleTypeFatal:
		return true
	default:
		return false
	}
}

// Rule configuration defines what kind of values are expected to exist in HTTP response, like status code or message in the body.
type Rule struct {
	Type              ResponseRuleType `hcl:"type,optional"`
	MatchingHTTPCodes []int            `hcl:"http_codes,optional"`
	MatchingBodyPart  string           `hcl:"body,optional"`
}

// Helper struct storing response HTTP status code and parsed response body
type response struct {
	Status       int
	StringStatus string
	Body         string
}

// HTTPTargetDriver holds a new client for writing messages to HTTP endpoints
type HTTPTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig

	client            *http.Client
	httpURL           string
	contentType       string
	headers           map[string]string
	basicAuthUsername string
	basicAuthPassword string
	log               *log.Entry
	dynamicHeaders    bool

	requestTemplate  *template.Template
	approxTmplSize   int
	responseRules    *ResponseRules
	metadataSafeMode bool // If enabled, we don't put response content into metadata reporting

	includeTimingHeaders bool
	rejectionThreshold   int
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

// GetDefaultConfiguration returns the default configuration for the HTTP target
func (ht *HTTPTargetDriver) GetDefaultConfiguration() any {
	return &HTTPTargetConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     50,
			MaxBatchBytes:        1048576,
			MaxMessageBytes:      1048576,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
		RequestTimeoutInMillis: 5000,
		EnableTLS:              false,

		ContentType: "application/json",
		ResponseRules: &ResponseRules{
			Rules: []Rule{},
		},
		MetadataSafeMode:           true,
		IncludeTimingHeaders:       false,
		RejectionThresholdInMillis: 150,
	}
}

// InitFromConfig creates initialises the HTTPTargetDriver from HTTPTargetConfig
func (ht *HTTPTargetDriver) InitFromConfig(cfg any) error {
	c, ok := cfg.(*HTTPTargetConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	// Our batching logic must account for template sizes, so we amend the batcher values to suit.
	approxTmplSize, requestTemplate, err := loadRequestTemplate(c.TemplateFile)
	if err != nil {
		return err
	}
	if approxTmplSize >= c.BatchingConfig.MaxBatchBytes || approxTmplSize >= c.BatchingConfig.MaxMessageBytes {
		return errors.New("target error: Template must be smaller than batching Byte limit. MaxBatchBytes: " + strconv.Itoa(c.BatchingConfig.MaxBatchBytes) + " MaxMessageBytes: " + strconv.Itoa(c.BatchingConfig.MaxMessageBytes))
	}

	ht.BatchingConfig = *c.BatchingConfig

	ht.BatchingConfig.MaxBatchBytes -= approxTmplSize
	ht.BatchingConfig.MaxMessageBytes -= approxTmplSize

	err = common.CheckURL(c.URL)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns

	tlsConfig, err2 := common.CreateTLSConfiguration(c.CertFile, c.KeyFile, c.CaFile, c.SkipVerifyTLS)
	if err2 != nil {
		return err2
	}

	if c.EnableTLS && tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}

	client := createHTTPClient(c.OAuth2ClientID, c.OAuth2ClientSecret, c.OAuth2TokenURL, c.OAuth2RefreshToken, transport)
	client.Timeout = time.Duration(c.RequestTimeoutInMillis) * time.Millisecond

	// validating response rules from config
	if c.ResponseRules != nil {
		for _, rule := range c.ResponseRules.Rules {
			if !isValidResponseRuleType(rule.Type) {
				return fmt.Errorf("target error: Invalid response rule type '%s'. Valid types are: 'invalid', 'setup'", rule.Type)
			}
		}
	}

	ht.client = client
	ht.httpURL = c.URL
	ht.contentType = c.ContentType
	ht.headers = c.Headers
	ht.basicAuthUsername = c.BasicAuthUsername
	ht.basicAuthPassword = c.BasicAuthPassword
	ht.log = log.WithFields(log.Fields{"target": SupportedTargetHTTP, "url": c.URL})
	ht.dynamicHeaders = c.DynamicHeaders
	ht.requestTemplate = requestTemplate
	ht.approxTmplSize = approxTmplSize
	ht.responseRules = c.ResponseRules
	ht.metadataSafeMode = c.MetadataSafeMode
	ht.includeTimingHeaders = c.IncludeTimingHeaders
	ht.rejectionThreshold = c.RejectionThresholdInMillis

	return nil
}

// GetBatchingConfig returns the batching config
func (ht *HTTPTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return ht.BatchingConfig
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (ht *HTTPTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {

	// If HTTP header feature is enabled, and headers are present, and the message is not oversized just send this message on its own, preserving current batch as is.
	if ht.dynamicHeaders && message.HTTPHeaders != nil && len(message.Data) <= ht.BatchingConfig.MaxMessageBytes {
		return []*models.Message{message}, currentBatch, nil
	}

	// Otherwise, perform batching as usual.
	return targetiface.DefaultBatcher(currentBatch, message, ht.BatchingConfig)
}

func (ht *HTTPTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	ht.log.Debugf("Writing %d messages to endpoint ...", len(messages))

	var reqBody []byte
	var goodMsgs []*models.Message
	var invalid []*models.Message

	if ht.requestTemplate != nil {
		reqBody, goodMsgs, invalid = ht.renderBatchUsingTemplate(messages)
	} else {
		reqBody, goodMsgs, invalid = ht.renderJSONArray(messages)
	}

	if len(goodMsgs) == 0 {
		// All messages failed validation - return them as invalid without error
		return models.NewTargetWriteResult(nil, nil, invalid), nil
	}

	request, err := http.NewRequest("POST", ht.httpURL, bytes.NewBuffer(reqBody))

	if err != nil {
		return models.NewTargetWriteResult(nil, nil, nil), models.FatalWriteError{Err: err}
	}

	request.Header.Add("Content-Type", ht.contentType)                        // Add content type
	addHeadersToRequest(request, ht.headers, ht.retrieveHeaders(goodMsgs[0])) // Add headers if there are any - because they're grouped by header, we just need to pick the header from one message
	if ht.basicAuthUsername != "" || ht.basicAuthPassword != "" {             // Add basic auth if either username or password is set
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
		response := response{Body: err.Error(), Status: 0, StringStatus: "Client failed to complete request"}

		newInvalid, failed, wrappedErr := handleResponseRules(response, ht.responseRules, goodMsgs, false) // Always metadata-safe

		// append with earlier invalids
		invalid = append(invalid, newInvalid...)

		return models.NewTargetWriteResult(nil, failed, invalid), wrappedErr
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
		}

		ht.log.Debugf("Successfully wrote %d/%d messages", len(goodMsgs), len(messages))
		return models.NewTargetWriteResult(goodMsgs, nil, invalid), nil
	}

	// Process non-2xx responses
	responseBody, err := io.ReadAll(resp.Body)

	if err != nil {
		response := response{Body: err.Error(), Status: 0, StringStatus: "Error reading response body"}
		newInvalid, failed, wrappedErr := handleResponseRules(response, ht.responseRules, goodMsgs, false) // Always metadata-safe
		invalid = append(invalid, newInvalid...)

		return models.NewTargetWriteResult(nil, failed, invalid), wrappedErr
	}

	response := response{Body: string(responseBody), Status: resp.StatusCode, StringStatus: resp.Status}

	newInvalid, failed, wrappedErr := handleResponseRules(response, ht.responseRules, goodMsgs, ht.metadataSafeMode)
	invalid = append(invalid, newInvalid...)

	return models.NewTargetWriteResult(nil, failed, invalid), wrappedErr
}

func handleResponseRules(response response, rules *ResponseRules, messages []*models.Message, safeMode bool) (invalid, failed []*models.Message, wrappedError error) {

	// Find first matching rule in order
	var matchedRule *Rule
	for _, rule := range rules.Rules {
		if ruleMatches(response, rule) {
			matchedRule = &rule
			break
		}
	}

	apiErr := &models.ApiError{
		StatusCode:   response.StringStatus,
		ResponseBody: response.Body,
		SafeMessage:  pickSafeMessage(response, matchedRule, safeMode),
	}
	for _, msg := range messages {
		msg.SetError(apiErr)
	}

	return categorizeByRuleType(matchedRule, messages, apiErr)
}

// pickSafeMessage returns an error description that is safe to log and report as metadata.
// When safeMode is off, returns the full response body. When on, returns the matched
// body part from the rule if available, otherwise a generic label based on the rule type.
func pickSafeMessage(response response, matchedRule *Rule, safeMode bool) string {
	if !safeMode {
		return response.Body
	}
	if matchedRule != nil {
		if matchedRule.MatchingBodyPart != "" {
			return matchedRule.MatchingBodyPart
		}
		switch matchedRule.Type {
		case ResponseRuleTypeInvalid:
			return "Invalid error"
		case ResponseRuleTypeThrottle:
			return "Throttle error"
		case ResponseRuleTypeSetup:
			return "Setup error"
		case ResponseRuleTypeFatal:
			return "Fatal error"
		}
	}
	return "Transient error"
}

// categorizeByRuleType splits messages into invalid or failed based on the matched rule type,
// and wraps the error details in the appropriate write error type for the router's retry logic.
func categorizeByRuleType(matchedRule *Rule, messages []*models.Message, apiErr *models.ApiError) (invalid, failed []*models.Message, wrappedError error) {
	errorDetails := fmt.Errorf("response status: '%s' with error details: '%s'", apiErr.StatusCode, apiErr.SafeMessage)

	if matchedRule != nil {
		switch matchedRule.Type {
		case ResponseRuleTypeInvalid:
			return messages, nil, nil

		case ResponseRuleTypeThrottle:
			return nil, messages, models.ThrottleWriteError{Err: errorDetails}

		case ResponseRuleTypeSetup:
			return nil, messages, models.SetupWriteError{Err: errorDetails}

		case ResponseRuleTypeFatal:
			return nil, messages, models.FatalWriteError{Err: errorDetails}
		}
	}

	// No rule matched - transient error
	return nil, messages, errorDetails
}

func ruleMatches(res response, rule Rule) bool {
	codeMatch := httpStatusMatches(res.Status, rule.MatchingHTTPCodes)
	if rule.MatchingBodyPart != "" {
		return codeMatch && responseBodyMatches(res.Body, rule.MatchingBodyPart)
	}
	return codeMatch
}

func httpStatusMatches(actual int, expectedCodes []int) bool {
	return slices.Contains(expectedCodes, actual)
}

func responseBodyMatches(actual string, bodyPattern string) bool {
	return strings.Contains(actual, bodyPattern)
}

// Open does nothing for this target
func (ht *HTTPTargetDriver) Open() error {
	return nil
}

// Close does nothing for this target
func (ht *HTTPTargetDriver) Close() {}

func (ht *HTTPTargetDriver) retrieveHeaders(msg *models.Message) map[string]string {
	if !ht.dynamicHeaders {
		return nil
	}

	return msg.HTTPHeaders
}

// renderBatchUsingTemplate creates a request from a batch of messages based on configured template
func (ht *HTTPTargetDriver) renderBatchUsingTemplate(messages []*models.Message) (templated []byte, success []*models.Message, invalid []*models.Message) {
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
func (ht *HTTPTargetDriver) renderJSONArray(messages []*models.Message) (templated []byte, success []*models.Message, invalid []*models.Message) {

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
