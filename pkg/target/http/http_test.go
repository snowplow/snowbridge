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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func createTestServerWithResponseCode(results *[][]byte, headers *http.Header, responseCode int, responseBody string, responseDelayMs int) *httptest.Server {
	mutex := &sync.Mutex{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer func() {
			if err := req.Body.Close(); err != nil {
				logrus.Error(err.Error())
			}
		}()
		time.Sleep(time.Duration(responseDelayMs) * time.Millisecond)
		data, err := io.ReadAll(req.Body)
		*headers = req.Header
		if err != nil {
			panic(err)
		}
		mutex.Lock()
		defer mutex.Unlock()
		*results = append(*results, data)
		w.WriteHeader(responseCode)
		if _, err := w.Write([]byte(responseBody)); err != nil {
			logrus.Error(err.Error())
		}
	}))
}

func createTestServer(results *[][]byte) *httptest.Server {
	var headers http.Header
	return createTestServerWithResponseCode(results, &headers, 200, "", 0)
}

func TestHTTP_HeadersFromConfig(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header
	wg := sync.WaitGroup{}
	server := createTestServerWithResponseCode(&results, &headers, 200, "", 0)
	defer server.Close()

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.Headers = map[string]string{
		"X-Custom-Header": "custom-value",
		"Accept-Language": "en-US",
	}
	err := driver.InitFromConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(1)
	ackFunc := func() { wg.Done() }
	messages := testutil.GetTestMessages(1, `{"message": "hello"}`, ackFunc)

	writeResult, err := driver.Write(messages)

	if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
		assert.Fail("Timed out waiting for acks")
	}

	assert.Nil(err)
	assert.Equal(1, len(writeResult.Sent))
	assert.Equal("custom-value", headers.Get("X-Custom-Header"))
	assert.Equal("en-US", headers.Get("Accept-Language"))
}

func TestHTTP_RetrieveHeaders(t *testing.T) {
	testCases := []struct {
		Name     string
		Msg      *models.Message
		Dynamic  bool
		Expected map[string]string
	}{
		{
			Name:     "message_headers_nil_dynamic_false",
			Msg:      &models.Message{},
			Dynamic:  false,
			Expected: nil,
		},
		{
			Name:     "message_headers_nil_dynamic_true",
			Msg:      &models.Message{},
			Dynamic:  true,
			Expected: nil,
		},
		{
			Name: "message_headers_empty_dynamic_false",
			Msg: &models.Message{
				HTTPHeaders: map[string]string{},
			},
			Dynamic:  false,
			Expected: nil,
		},
		{
			Name: "message_headers_empty_dynamic_true",
			Msg: &models.Message{
				HTTPHeaders: map[string]string{},
			},
			Dynamic:  true,
			Expected: map[string]string{},
		},
		{
			Name: "message_headers_non_empty_dynamic_false",
			Msg: &models.Message{
				HTTPHeaders: map[string]string{
					"foo": "bar",
				},
			},
			Dynamic:  false,
			Expected: nil,
		},
		{
			Name: "message_headers_non_empty_dynamic_true",
			Msg: &models.Message{
				HTTPHeaders: map[string]string{
					"foo": "bar",
				},
			},
			Dynamic:  true,
			Expected: map[string]string{"foo": "bar"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			driver := &HTTPTargetDriver{}
			config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
			config.URL = "http://test"
			config.DynamicHeaders = tt.Dynamic
			err := driver.InitFromConfig(config)
			if err != nil {
				t.Fatalf("failed to create test target: %s", err.Error())
			}

			out := driver.retrieveHeaders(tt.Msg)
			if !reflect.DeepEqual(out, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(out),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestHTTP_RequestTimeoutsConfig(t *testing.T) {
	testCases := []struct {
		Name                  string
		Config                *HTTPTargetConfig
		ExpectedClientTimeout time.Duration
	}{
		{
			Name:                  "Default",
			Config:                &HTTPTargetConfig{},
			ExpectedClientTimeout: time.Duration(5) * time.Second,
		},
		{
			Name:                  "Custom millis",
			Config:                &HTTPTargetConfig{RequestTimeoutInMillis: 2500},
			ExpectedClientTimeout: time.Duration(2500) * time.Millisecond,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			driver := &HTTPTargetDriver{}
			config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
			config.URL = "http://test"
			if tt.Config.RequestTimeoutInMillis != 0 {
				config.RequestTimeoutInMillis = tt.Config.RequestTimeoutInMillis
			}
			config.BatchingConfig.MaxBatchBytes = 1048576
			config.BatchingConfig.MaxMessageBytes = 1048576
			_ = driver.InitFromConfig(config)

			assert.Equal(tt.ExpectedClientTimeout, driver.client.Timeout)
		})
	}
}

func TestHTTP_AddHeadersToRequest(t *testing.T) {
	assert := assert.New(t)

	req, err := http.NewRequest("POST", "abc", bytes.NewBuffer([]byte("def")))
	if err != nil {
		t.Fatal(err)
	}
	headersToAdd := map[string]string{"Max Forwards": "10", "Accept-Language": "en-US,en-IE", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}

	expectedHeaders := http.Header{
		"Max Forwards":    []string{"10"},
		"Accept-Language": []string{"en-US,en-IE"},
		"Accept-Datetime": []string{"Thu, 31 May 2007 20:35:00 GMT"},
	}

	addHeadersToRequest(req, headersToAdd, nil)
	assert.Equal(expectedHeaders, req.Header)

	req2, err2 := http.NewRequest("POST", "abc", bytes.NewBuffer([]byte("def")))
	if err2 != nil {
		t.Fatal(err2)
	}
	var noHeadersToAdd map[string]string
	noHeadersExpected := http.Header{}

	addHeadersToRequest(req2, noHeadersToAdd, nil)

	assert.Equal(noHeadersExpected, req2.Header)
}

func TestHTTP_AddHeadersToRequest_WithDynamicHeaders(t *testing.T) {
	testCases := []struct {
		Name           string
		ConfigHeaders  map[string]string
		DynamicHeaders map[string]string
		ExpectedHeader http.Header
	}{
		{
			Name:           "config_nil_dynamic_nil",
			ConfigHeaders:  nil,
			DynamicHeaders: nil,
			ExpectedHeader: http.Header{},
		},
		{
			Name:           "config_nil_dynamic_empty",
			ConfigHeaders:  nil,
			DynamicHeaders: map[string]string{},
			ExpectedHeader: http.Header{},
		},
		{
			Name:          "config_nil_dynamic_yes",
			ConfigHeaders: nil,
			DynamicHeaders: map[string]string{
				"Content-Length": "0",
			},
			ExpectedHeader: http.Header{
				"Content-Length": {"0"},
			},
		},
		{
			Name: "config_yes_dynamic_nil",
			ConfigHeaders: map[string]string{
				"Max Forwards": "10",
			},
			DynamicHeaders: nil,
			ExpectedHeader: http.Header{
				"Max Forwards": {"10"},
			},
		},
		{
			Name: "config_yes_dynamic_empty",
			ConfigHeaders: map[string]string{
				"Max Forwards": "10",
			},
			DynamicHeaders: map[string]string{},
			ExpectedHeader: http.Header{
				"Max Forwards": {"10"},
			},
		},
		{
			Name: "config_yes_dynamic_yes",
			ConfigHeaders: map[string]string{
				"Max Forwards": "10",
			},
			DynamicHeaders: map[string]string{
				"Content-Length": "0",
			},
			ExpectedHeader: http.Header{
				"Max Forwards":   {"10"},
				"Content-Length": {"0"},
			},
		},
		{
			Name: "config_yes_dynamic_yes_same_key",
			ConfigHeaders: map[string]string{
				"Max Forwards":   "10",
				"Content-Length": "0",
			},
			DynamicHeaders: map[string]string{
				"Content-Length": "1",
				"Test-Header":    "test",
			},
			ExpectedHeader: http.Header{
				"Max Forwards":   {"10"},
				"Content-Length": {"0", "1"},
				"Test-Header":    {"test"},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			req, err := http.NewRequest("POST", "abc", nil)
			if err != nil {
				t.Fatalf("failed to create test http.Request")
			}

			addHeadersToRequest(req, tt.ConfigHeaders, tt.DynamicHeaders)
			assert.Equal(tt.ExpectedHeader, req.Header)
		})
	}
}

func TestHTTP_NewHTTPTarget(t *testing.T) {
	assert := assert.New(t)
	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = "http://something"
	err := driver.InitFromConfig(config)
	assert.Nil(err)
	assert.NotNil(driver)

	driver2 := &HTTPTargetDriver{}
	config2 := driver2.GetDefaultConfiguration().(*HTTPTargetConfig)
	config2.URL = "something"
	err = driver2.InitFromConfig(config2)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("invalid url for HTTP target: 'something'", err.Error())
	}

	driver3 := &HTTPTargetDriver{}
	config3 := driver3.GetDefaultConfiguration().(*HTTPTargetConfig)
	config3.URL = ""
	err = driver3.InitFromConfig(config3)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("invalid url for HTTP target: ''", err.Error())
	}
}

func TestHTTP_Write_Simple_Batch(t *testing.T) {
	testCases := []struct {
		Name         string
		ResponseCode int
		BatchSize    int
	}{
		{Name: "200 response Code", ResponseCode: 200},
		{Name: "201 response Code", ResponseCode: 201},
		{Name: "226 response Code", ResponseCode: 226},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var results [][]byte
			var headers http.Header
			wg := sync.WaitGroup{}
			server := createTestServerWithResponseCode(&results, &headers, tt.ResponseCode, "", 0)
			defer server.Close()

			driver := &HTTPTargetDriver{}
			config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
			config.URL = server.URL
			err := driver.InitFromConfig(config)
			if err != nil {
				t.Fatal(err)
			}

			var ackOps int64
			ackFunc := func() {
				atomic.AddInt64(&ackOps, 1)

				wg.Done()
			}

			goodMessages := testutil.GetTestMessages(25, `{"message": "Hello Server!!"}`, ackFunc)
			badMessages := testutil.GetTestMessages(3, `{"message": "Hello Server!!"`, ackFunc) // invalids

			messages := append(goodMessages, badMessages...)

			wg.Add(25)
			writeResult, err1 := driver.Write(messages)

			if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
				assert.Fail("Timed out waiting for acks")
			}

			assert.Nil(err1)
			assert.Equal(int64(25), int64(len(writeResult.Sent)))
			assert.Equal(int64(0), int64(len(writeResult.Failed)))

			assert.Equal(25, len(writeResult.Sent))

			// 1 request = 1 result
			assert.Equal(1, len(results))

			// Unmarshal the result and check that it's a batch of 25 of our messages
			if len(results) > 0 {
				var result []map[string]string
				err := json.Unmarshal(results[0], &result)
				if err != nil {
					assert.Fail("Failed to unmarshal result: " + err.Error())
				}
				assert.Equal(25, len(result))
				for _, entry := range result {
					passed := assert.Equal("Hello Server!!", entry["message"])
					if !passed {
						// For readability of output. We just need one to fail to know it failed.
						break
					}
				}
			}

			assert.Equal(3, len(writeResult.Invalid)) // invalids went to the right place
			for _, msg := range writeResult.Invalid {
				// Check all invalids have error as expected
				assert.Regexp("Message can't be parsed as valid JSON: .*", msg.GetError().Error())
			}

			assert.Equal(int64(25), ackOps)
		})
	}
}

func TestHTTP_Write_Simple_Single(t *testing.T) {
	testCases := []struct {
		Name         string
		ResponseCode int
		BatchSize    int
	}{
		{Name: "200 response Code", ResponseCode: 200},
		{Name: "201 response Code", ResponseCode: 201},
		{Name: "226 response Code", ResponseCode: 226},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var results [][]byte
			var headers http.Header
			wg := sync.WaitGroup{}
			server := createTestServerWithResponseCode(&results, &headers, tt.ResponseCode, "", 0)
			defer server.Close()

			driver := &HTTPTargetDriver{}
			config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
			config.URL = server.URL
			err := driver.InitFromConfig(config)
			if err != nil {
				t.Fatal(err)
			}

			var ackOps int64
			ackFunc := func() {
				atomic.AddInt64(&ackOps, 1)

				wg.Done()
			}

			messages := testutil.GetTestMessages(1, `{"message": "Hello Server!!"}`, ackFunc)

			wg.Add(1)
			writeResult, err1 := driver.Write(messages)

			if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
				assert.Fail("Timed out waiting for acks")
			}

			assert.Nil(err1)
			assert.Equal(int64(1), int64(len(writeResult.Sent)))
			assert.Equal(int64(0), int64(len(writeResult.Failed)))

			assert.Equal(1, len(writeResult.Sent))
			assert.Equal(1, len(results))
			for _, result := range results {
				assert.Equal(`[{"message":"Hello Server!!"}]`, string(result))
			}

			assert.Equal(int64(1), ackOps)
		})
	}
}

func TestHTTP_Write_Concurrent(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results)
	defer server.Close()

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	err := driver.InitFromConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	mu := &sync.Mutex{}
	var ackOps int64
	ackFunc := func() {
		mu.Lock()
		atomic.AddInt64(&ackOps, 1)
		mu.Unlock()
		wg.Done()
	}

	messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)

	for _, message := range messages {
		wg.Add(1)
		go func(msg *models.Message) {
			writeResult, err1 := driver.Write([]*models.Message{msg})
			assert.Nil(err1)
			assert.Equal(1, len(writeResult.Sent))
		}(message)
	}

	if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
		assert.Fail("Timed out waiting for acks")
	}

	assert.Equal(10, len(results))
	for _, result := range results {
		assert.Equal(`[{"message":"Hello Server!!"}]`, string(result))
	}

	assert.Equal(int64(10), ackOps)
}

func TestHTTP_Write_Failure(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	server := createTestServer(&results)
	defer server.Close()

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = "http://NonexistentEndpoint"
	config.BatchingConfig.MaxBatchMessages = 1
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)

	writeResult, err1 := driver.Write(messages)

	assert.NotNil(err1)
	if err1 != nil {
		assert.Regexp("response status:.*", err1.Error())
	}

	assert.Equal(int64(0), int64(len(writeResult.Sent)))
	assert.Equal(int64(10), int64(len(writeResult.Failed)))
	assert.Equal(10, len(writeResult.Failed))
	assert.Empty(writeResult.Sent)
}

func TestHTTP_Write_InvalidResponseCode(t *testing.T) {
	testCases := []struct {
		Name         string
		ResponseCode int
	}{
		{Name: "300 response Code", ResponseCode: 300},
		{Name: "400 response Code", ResponseCode: 400},
		{Name: "503 response Code", ResponseCode: 503},
	}
	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var results [][]byte
			var headers http.Header
			server := createTestServerWithResponseCode(&results, &headers, tt.ResponseCode, "", 0)
			defer server.Close()

			driver := &HTTPTargetDriver{}
			config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
			config.URL = server.URL
			config.BatchingConfig.MaxBatchMessages = 1
			err := driver.InitFromConfig(config)
			if err != nil {
				t.Fatal(err)
			}

			var ackOps int64
			ackFunc := func() {
				atomic.AddInt64(&ackOps, 1)
			}

			messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)
			writeResult, err1 := driver.Write(messages)

			assert.NotNil(err1)
			if err1 != nil {
				assert.Regexp("response status:.*", err1.Error())
			}

			assert.Equal(int64(0), int64(len(writeResult.Sent)))
			assert.Equal(int64(10), int64(len(writeResult.Failed)))
			assert.Equal(10, len(writeResult.Failed))
			assert.Empty(writeResult.Sent)
		})
	}
}

func TestHTTP_Write_EnabledTemplating(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results)
	defer server.Close()

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.TemplateFile = string(`../../../integration/http/template`)
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
		wg.Done()
	}

	goodMessages := testutil.GetTestMessages(3, `{ "event_data": { "nested": "value1"}, "attribute_data": 1}`, ackFunc)
	badMessages := testutil.GetTestMessages(3, `{ "event_data": { "nested": "value1"},`, ackFunc) // invalid

	messages := append(goodMessages, badMessages...)

	wg.Add(3)
	writeResult, err1 := driver.Write(messages)
	wg.Wait()

	assert.Nil(err1)
	assert.Equal(int64(3), int64(len(writeResult.Sent)))
	assert.Equal(int64(0), int64(len(writeResult.Failed)))
	assert.Equal(3, len(writeResult.Sent))
	assert.Equal(3, len(writeResult.Invalid)) // invalids went to the right place
	for _, msg := range writeResult.Invalid {
		// Invalids have errors as expected
		assert.Regexp("Message can't be parsed as valid JSON: .*", msg.GetError().Error())
	}
	assert.Equal(1, len(results))

	expectedOutput := "{\n  \"attributes\": [1,1,1],\n  \"events\": [{\"nested\":\"value1\"},{\"nested\":\"value1\"},{\"nested\":\"value1\"}]\n}\n"
	assert.Equal(expectedOutput, string(results[0]))
	assert.Equal(int64(3), ackOps)
}

// Steps to create certs manually:

// openssl genrsa -out rootCA.key 4096
// openssl req -x509 -new -key rootCA.key -days 3650 -out rootCA.crt -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost"

// openssl genrsa -out localhost.key 2048
// openssl req -new -key localhost.key -out localhost.csr -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost"
// openssl x509 -req -in localhost.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -days 365 -out localhost.crt

func TestHTTP_Write_Invalid(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 400, "Request is invalid. Invalid value for field 'attribute'", 0)
	defer server.Close()

	responseRules := ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400, 401}, MatchingBodyPart: "Invalid value for field 'attribute'"},
		},
	}

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.TemplateFile = string(`../../../integration/http/template`)
	config.ResponseRules = &responseRules
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	writeResult, err1 := driver.Write(input)

	assert.Nil(err1)
	assert.Equal(0, len(writeResult.Sent))
	assert.Equal(1, len(writeResult.Invalid))
	assert.Equal("HTTP Status Code: 400 Bad Request Body: Request is invalid. Invalid value for field 'attribute'", writeResult.Invalid[0].GetError().Error())
}

func TestHTTP_Write_Setup(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 401, "Authentication issue. Invalid token", 0)
	defer server.Close()

	responseRules := ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleTypeSetup, MatchingHTTPCodes: []int{401}, MatchingBodyPart: "Invalid token"},
		},
	}

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.TemplateFile = string(`../../../integration/http/template`)
	config.ResponseRules = &responseRules
	err := driver.InitFromConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	writeResult, err := driver.Write(input)

	assert.Equal(0, len(writeResult.Sent))
	assert.Equal(0, len(writeResult.Invalid))
	assert.Equal(1, len(writeResult.Failed))

	_, isSetup := err.(models.SetupWriteError)
	assert.True(isSetup)
	assert.Regexp(".*response status: '401 Unauthorized' with error details: 'Invalid token'", err.Error())
}

func TestHTTP_Write_Throttle(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 429, "Rate limit exceeded", 0)
	defer server.Close()

	responseRules := ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleTypeThrottle, MatchingHTTPCodes: []int{429}, MatchingBodyPart: "Rate limit"},
		},
	}

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.TemplateFile = string(`../../../integration/http/template`)
	config.ResponseRules = &responseRules

	err := driver.InitFromConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}
	writeResult, err := driver.Write(input)

	assert.Equal(0, len(writeResult.Sent))
	assert.Equal(0, len(writeResult.Invalid))
	assert.Equal(1, len(writeResult.Failed))

	_, isThrottle := err.(models.ThrottleWriteError)
	assert.True(isThrottle)
	assert.Regexp(".*response status: '429.*' with error details: 'Rate limit'", err.Error())
}

func TestHTTP_Write_ClientTimeoutResponseRule(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 200, "ok", 10)
	defer server.Close()

	responseRules := ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{0}, MatchingBodyPart: "context deadline exceeded"},
		},
	}

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.TemplateFile = string(`../../../integration/http/template`)
	config.ResponseRules = &responseRules
	config.RequestTimeoutInMillis = 1
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	writeResult, err1 := driver.Write(input)

	assert.Nil(err1)
	assert.Equal(0, len(writeResult.Sent), "Sent count should be 0")
	assert.Equal(0, len(writeResult.Failed), "Failed count should be 0")
	// fmt.Println(writeResult.Failed[0].GetError().Error())
	assert.Equal(1, len(writeResult.Invalid), "Invalid count should be 1")
	assert.Contains(writeResult.Invalid[0].GetError().Error(), "Client failed to complete request")
	assert.Contains(writeResult.Invalid[0].GetError().Error(), "context deadline exceeded")
}

func TestHTTP_Write_ClientTimeoutTransient(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 200, "ok", 10)
	defer server.Close()

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.TemplateFile = string(`../../../integration/http/template`)
	config.RequestTimeoutInMillis = 1
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	writeResult, err1 := driver.Write(input)

	assert.NotNil(err1)
	assert.Equal(0, len(writeResult.Sent), "Sent count should be 0")
	assert.Equal(1, len(writeResult.Failed), "Failed count should be 1")
	assert.Equal(0, len(writeResult.Invalid), "Invalid count should be 0")
	// The error should contain the actual timeout cause, not just the status
	assert.Contains(err1.Error(), "Client failed to complete request")
	assert.Contains(err1.Error(), "context deadline exceeded")
}

func TestHTTP_TimeOrientedHeadersEnabled(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 200, "ok", 0)
	defer server.Close()

	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.RequestTimeoutInMillis = 1000
	config.RejectionThresholdInMillis = 100
	config.IncludeTimingHeaders = true
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	beforeRequest := time.Now().UTC().UnixMilli()
	if _, err := driver.Write(input); err != nil {
		logrus.Error(err.Error())
	}
	afterRequest := time.Now().UTC().UnixMilli()

	rejectionTimestamp, err := strconv.ParseInt(headers.Get("Rejection-Timestamp"), 10, 64)

	assert.Empty(err)
	assert.GreaterOrEqual(rejectionTimestamp, beforeRequest)
	assert.LessOrEqual(rejectionTimestamp, afterRequest+900) // based on configured timeout and threshold
}

func TestHTTP_Write_TLS(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Test that https requests work with manually provided certs
	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = "https://localhost:8999/hello"
	config.EnableTLS = true
	config.CertFile = string(`../../../integration/http/localhost.crt`)
	config.KeyFile = string(`../../../integration/http/localhost.key`)
	config.CaFile = string(`../../../integration/http/rootCA.crt`)
	err := driver.InitFromConfig(config)

	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)

	writeResult, err1 := driver.Write(messages)

	assert.Nil(err1)
	assert.Equal(10, len(writeResult.Sent))

	assert.Equal(int64(10), ackOps)

	ngrokAddress := getNgrokAddress() + "/hello"

	// Test that https requests work for different endpoints when different certs are provided manually
	driver2 := &HTTPTargetDriver{}
	config2 := driver2.GetDefaultConfiguration().(*HTTPTargetConfig)
	config2.URL = ngrokAddress
	config2.EnableTLS = true
	config2.CertFile = string(`../../../integration/http/localhost.crt`)
	config2.KeyFile = string(`../../../integration/http/localhost.key`)
	config2.CaFile = string(`../../../integration/http/rootCA.crt`)
	err2 := driver2.InitFromConfig(config2)

	if err2 != nil {
		t.Fatal(err2)
	}

	writeResult2, err3 := driver2.Write(messages)

	assert.Nil(err3)
	assert.Equal(10, len(writeResult2.Sent))

	assert.Equal(int64(20), ackOps)

	// Test that https works when certs aren't manually provided
	driver3 := &HTTPTargetDriver{}
	config3 := driver3.GetDefaultConfiguration().(*HTTPTargetConfig)
	config3.URL = ngrokAddress
	err4 := driver3.InitFromConfig(config3)
	if err4 != nil {
		t.Fatal(err4)
	}

	writeResult3, err5 := driver3.Write(messages)

	assert.Nil(err5)
	assert.Equal(10, len(writeResult3.Sent))

	assert.Equal(int64(30), ackOps)
}

func TestHTTP_ProvideRequestBody(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTargetDriver{}

	inputMessages := []*models.Message{
		{Data: []byte(`{"key": "value1"}`)},
		{Data: []byte(`{"key": "value2"}`)},
		{Data: []byte(`{"key": "value3"}`)},
		{Data: []byte(`justastring`)},
	}

	templated, success, invalid := target.renderJSONArray(inputMessages)

	assert.Equal(`[{"key":"value1"},{"key":"value2"},{"key":"value3"}]`, string(templated))
	assert.Equal(3, len(success))
	assert.Equal(1, len(invalid))
	assert.Regexp("Message can't be parsed as valid JSON: .*", invalid[0].GetError().Error())
}

func TestHTTP_Write_DynamicHeadersAttached(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header
	server := createTestServerWithResponseCode(&results, &headers, 200, "", 0)
	defer server.Close()

	//dynamicHeaders enabled
	driver := &HTTPTargetDriver{}
	config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	config.URL = server.URL
	config.DynamicHeaders = true
	err := driver.InitFromConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	ackFunc := func() {
		wg.Done()
	}

	// We're sending 2 events
	wg.Add(2)

	inputMessages := []*models.Message{
		{Data: []byte(`{"key": "value3"}`), AckFunc: ackFunc, HTTPHeaders: map[string]string{"h1": "v1"}},
		{Data: []byte(`{"key": "value4"}`), AckFunc: ackFunc, HTTPHeaders: map[string]string{"h1": "v1"}},
	}

	writeResult, err1 := driver.Write(inputMessages)

	if ok := WaitForAcksWithTimeout(1*time.Second, &wg); !ok {
		assert.Fail("Timed out waiting for acks")
	}

	assert.Nil(err1)
	assert.Equal(2, len(writeResult.Sent))
	assert.Equal(1, len(results))

	assert.Contains(results, []byte(`[{"key":"value3"},{"key":"value4"}]`))

	// Check that the header was attached.
	assert.Equal("v1", headers.Get("h1"))

}

type ngrokAPIObject struct {
	PublicURL string `json:"public_url"`
	Proto     string `json:"proto"`
}

type ngrokAPIResponse struct {
	Tunnels []ngrokAPIObject `json:"tunnels"`
}

// Query ngrok api for endpoint to hit
func getNgrokAddress() string {
	var resp *http.Response
	var err error
	for range 10 { // retry 10 times as this part is flaky
		resp, err = http.DefaultClient.Get("http://localhost:4040/api/tunnels")
		if resp != nil {
			err = nil
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		panic(err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var ngrokResponse ngrokAPIResponse
	if err := json.Unmarshal(body, &ngrokResponse); err != nil {
		logrus.Error(err.Error())
	}

	for _, obj := range ngrokResponse.Tunnels {
		if obj.Proto == "https" {
			return obj.PublicURL
		}
	}
	panic("no ngrok https endpoint found")
}

func WaitForAcksWithTimeout(timeout time.Duration, wg *sync.WaitGroup) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return true
	case <-time.After(timeout):
		return false
	}
}
