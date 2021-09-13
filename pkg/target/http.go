// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

type HttpTarget struct {
	client            *http.Client
	httpUrl           string
	byteLimit         int // Do we need this? Assuming targets might have varying limits on the server side I think best to include.
	contentType       string
	headers           [][]string
	basicAuthUsername string
	basicAuthPassword string
	log               *log.Entry
	// TODO: Add a means of configuring things like headers and certs
}

func checkUrl(str string) error {
	u, err := url.Parse(str)
	if err != nil {
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		return errors.New(fmt.Sprintf("Invalid url for Http target: '%s'", str))
	}
	return nil
}

// We use `::` and `,,` as separators so that we may have `:` and `,` characters in the header content, (which seems common).
// Another option is to use single characters but expect escapes in the provided values (which would then need to be unescaped later)
// Feels like dealing the pain in config is easier than in code, but perhaps it's too jarring.
func getHeaders(headers string) ([][]string, error) {
	if headers == "" { // No headers is acceptable
		return nil, nil
	}
	var out [][]string
	// Split provided string by comma
	for _, entry := range strings.Split(headers, ",,") {
		// Then split by colon and throw an error if we don't get what we expect
		pair := strings.Split(entry, "::")
		if len(pair) != 2 {
			return nil, errors.New("Error parsting headers. Ensure that headers are provided in `{name1}::{value1},,{name2}::{value2}` format")
		}
		out = append(out, pair)
	}
	return out, nil
}

func addHeadersToRequest(request *http.Request, headers [][]string) {
	if headers == nil {
		return
	}
	for _, header := range headers {
		request.Header.Add(header[0], header[1])
	}
}

// TODO: Add basicauth username and password
func NewHttpTarget(httpUrl string, requestTimeout int, byteLimit int, contentType string, headers string, basicAuthUsername string, basicAuthPassword string,
	certFile string, keyFile string, caFile string, skipVerifyTLS bool) (*HttpTarget, error) {
	err := checkUrl(httpUrl)
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
	return &HttpTarget{
		client: &http.Client{
			Transport: transport,
			Timeout:   time.Duration(requestTimeout) * time.Second,
		},
		httpUrl:           httpUrl,
		byteLimit:         byteLimit,
		contentType:       contentType,
		headers:           parsedHeaders,
		basicAuthUsername: basicAuthUsername,
		basicAuthPassword: basicAuthPassword,
		log:               log.WithFields(log.Fields{"target": "http", "url": httpUrl}),
	}, nil
}

func (ht *HttpTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
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

		request, err := http.NewRequest("POST", ht.httpUrl, bytes.NewBuffer(msg.Data))
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
			errResult = multierror.Append(errResult, errors.New(resp.Status)) // Should the response body go into the error too?
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
func (ht *HttpTarget) Open() {}

// Close does nothing for this target
func (ht *HttpTarget) Close() {}

func (ht *HttpTarget) MaximumAllowedMessageSizeBytes() int {
	return ht.byteLimit
}

func (ht *HttpTarget) GetID() string { // TODO: Is just the url the best thing to return for getID? Should it just do nothing instead?
	return ht.httpUrl
}
