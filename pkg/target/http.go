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
	"text/template"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/common"
	"github.com/snowplow/snowbridge/pkg/models"

	"golang.org/x/oauth2"
)

// HTTPTargetConfig configures the destination for records consumed
type HTTPTargetConfig struct {
	HTTPURL string `hcl:"url" env:"TARGET_HTTP_URL"`

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

	RequestMaxMessages int `hcl:"request_max_messages,optional"`
	RequestByteLimit   int `hcl:"request_byte_limit,optional"` // note: breaking change here
	MessageByteLimit   int `hcl:"message_byte_limit,optional"`
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
func newHTTPTarget(
	httpURL string,
	requestTimeout int,
	requestMaxMessages int,
	requestByteLimit int,
	messageByteLimit int,
	contentType string,
	headers string,
	basicAuthUsername string,
	basicAuthPassword string,
	certFile string,
	keyFile string,
	caFile string,
	skipVerifyTLS bool,
	dynamicHeaders bool,
	oAuth2ClientID string,
	oAuth2ClientSecret string,
	oAuth2RefreshToken string,
	oAuth2TokenURL string) (*HTTPTarget, error) {
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
		contentType:       contentType,
		headers:           parsedHeaders,
		basicAuthUsername: basicAuthUsername,
		basicAuthPassword: basicAuthPassword,
		log:               log.WithFields(log.Fields{"target": "http", "url": httpURL}),
		dynamicHeaders:    dynamicHeaders,

		requestMaxMessages: requestMaxMessages,
		requestByteLimit:   requestByteLimit,
		messageByteLimit:   messageByteLimit,
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
		c.RequestMaxMessages,
		c.RequestByteLimit,
		c.MessageByteLimit,
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
		RequestMaxMessages: 20,
		RequestByteLimit:   1048576,
		MessageByteLimit:   1048576,

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

	chunks, oversized := models.GetChunkedMessages(
		messages,
		ht.requestMaxMessages,
		ht.messageByteLimit,
		ht.requestByteLimit,
	)

	sent := []*models.Message{}
	failed := []*models.Message{}
	var errResult error

	for _, chunk := range chunks {
		grouped := ht.groupByDynamicHeaders(chunk)

		for _, group := range grouped {
			var reqBody []byte
			var goodMsgs []*models.Message
			var badMsgs []*models.Message
			var err error

			// just for now to spike
			templaterConfigured := false
			if templaterConfigured {
				reqBody, goodMsgs, badMsgs, err = ht.requestTemplater(templ, group)
			} else {
				reqBody, goodMsgs, badMsgs, err = ht.provideRequestBody(group)
			}
			failed = append(failed, badMsgs...)
			if err != nil {
				errResult = multierror.Append(errResult, errors.New("Error constructing request"))
				continue
			}
			if len(goodMsgs) == 0 {
				continue
			}

			request, err := http.NewRequest("POST", ht.httpURL, bytes.NewBuffer(reqBody))
			if err != nil {
				failed = append(failed, goodMsgs...)
				errResult = errors.Wrap(errResult, "Error creating request: "+err.Error())
				continue
			}
			request.Header.Add("Content-Type", ht.contentType)                        // Add content type
			addHeadersToRequest(request, ht.headers, ht.retrieveHeaders(goodMsgs[0])) // Add headers if there are any - because they're grouped by header, we just need to pick the header from one message
			if ht.basicAuthUsername != "" && ht.basicAuthPassword != "" {             // Add basic auth if set
				request.SetBasicAuth(ht.basicAuthUsername, ht.basicAuthPassword)
			}
			requestStarted := time.Now()
			resp, err := ht.client.Do(request) // Make request
			requestFinished := time.Now()

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
			defer resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				for _, msg := range goodMsgs {
					if msg.AckFunc != nil { // Ack successful messages
						msg.AckFunc()
					}
					sent = append(sent, msg)
				}
			} else {
				errResult = multierror.Append(errResult, errors.New("Got response status: "+resp.Status))
				failed = append(failed, goodMsgs...)
				continue
			}
		}
	}

	ht.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return &models.TargetWriteResult{
		Sent:      sent,
		Failed:    failed,
		Oversized: oversized,
		Invalid:   nil,
		// TODO: design decision: Where we cannot create a request body, here we treat that as 'failed', but technically it may be considered 'invalid'. We should validate this design decision.
	}, errResult
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

// requestTemplater creates a request from a batch of messages
func (ht *HTTPTarget) requestTemplater(tmpl string, messages []*models.Message) (templated []byte, success []*models.Message, failed []*models.Message, err error) {
	invalid := make([]*models.Message, 0)
	safe := make([]*models.Message, 0)

	formatted := []map[string]interface{}{}
	for _, msg := range messages {
		// Use json.RawMessage to ensure templating format works (real implementation has a problem to figure out here)
		var asMap map[string]interface{}

		if err := json.Unmarshal(msg.Data, &asMap); err != nil {
			msg.SetError(errors.Wrap(err, "templater error")) // TODO: Cleanup!
			invalid = append(invalid, msg)
			continue
		}

		formatted = append(formatted, asMap)
	}
	var buf bytes.Buffer

	customFunctions := template.FuncMap{
		// If you use this in your template on struct-like fields, you get rendered nice JSON `{"field":"value"}` instead of stringified map `map[field:value]`
		// TODO: This works for now but we should check if there is more efficient solution.
		"asJson": func(v interface{}) string {
			a, _ := json.Marshal(v)
			return string(a)
		},
	}

	//TODO parse when creating target
	t := template.Must(template.New("example").Funcs(customFunctions).Parse(tmpl))
	if err := t.Execute(&buf, formatted); err != nil {

		invalid = append(invalid, safe...)

		return nil, nil, invalid, err
	}

	return buf.Bytes(), safe, nil, nil
}

// Where no transformation function provides a request body, we must provide one - this necessarily must happen last.
// This is a http specific function so we define it here to avoid scope for misconfiguration
func (ht *HTTPTarget) provideRequestBody(messages []*models.Message) (templated []byte, success []*models.Message, failed []*models.Message, err error) {

	// TODO: Add test for when messagess are just strings & confirm that it all works

	// TODO: Note: This would mean that the GTM client gets arrays of single events instead of single events.
	// But we could configure an explicit templater to change that if we wanted
	// We should test to be certain that it's still compatible.

	requestData := []string{}
	for _, msg := range messages {
		requestData = append(requestData, string(msg.Data))
	}
	// TODO: Add tests to be sure this produces the desired request
	requestBody, err := json.Marshal(requestData)
	if err != nil {
		// TODO: Handle errors here
		return nil, nil, messages, err
	}

	return requestBody, messages, nil, nil
}

const templ = `{
	attributes: [ {{$first_1 := true}}
	  {{range .}}{{if $first_1}}{{$first_1 = false}}{{else}},{{end}}
	  {{printf "%s" .attribute_data}}{{end}}
	  ],
	events: [ {{$first_2 := true}}
	  {{range .}}{{if $first_2}}{{$first_2 = false}}{{else}},{{end}}
	  {{printf "%s" .event_data}}{{end}}
	  ]
  }`

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
