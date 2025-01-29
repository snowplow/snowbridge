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
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func createTestServerWithResponseCode(results *[][]byte, headers *http.Header, responseCode int, responseBody string) *httptest.Server {
	mutex := &sync.Mutex{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		data, err := io.ReadAll(req.Body)
		*headers = req.Header
		if err != nil {
			panic(err)
		}
		mutex.Lock()
		*results = append(*results, data)
		w.WriteHeader(responseCode)
		w.Write([]byte(responseBody))
		mutex.Unlock()
	}))
}

func createTestServer(results *[][]byte) *httptest.Server {
	var headers http.Header
	return createTestServerWithResponseCode(results, &headers, 200, "")
}

func TestHTTP_GetHeaders(t *testing.T) {
	assert := assert.New(t)
	valid1 := `{"Max Forwards": "10", "Accept-Language": "en-US", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}`

	expected1 := map[string]string{"Max Forwards": "10", "Accept-Language": "en-US", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}

	out1, err1 := getHeaders(valid1)

	assert.Nil(err1)
	assert.Equal(expected1, out1)

	valid2 := `{"Max Forwards": "10"}`
	expected2 := map[string]string{"Max Forwards": "10"}

	out2, err2 := getHeaders(valid2)

	assert.Nil(err2)
	assert.Equal(expected2, out2)

	valid3 := "{\"Max Forwards\": \"10\", \"Accept-Language\": \"en-US\", \"Accept-Datetime\": \"Thu, 31 May 2007 20:35:00 GMT\"}"

	expected3 := map[string]string{"Max Forwards": "10", "Accept-Language": "en-US", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}

	out3, err3 := getHeaders(valid3)

	assert.Nil(err3)
	assert.Equal(expected3, out3)

	invalid1 := `{"Max Forwards": 10}`
	out4, err4 := getHeaders(invalid1)

	assert.NotNil(err4)
	if err4 != nil {
		assert.Equal("Error parsing headers. Ensure that headers are provided as a JSON of string key-value pairs: json: cannot unmarshal number into Go value of type string", err4.Error())
	}
	assert.Nil(out4)

	invalid2 := `[{"Max Forwards": "10"}]`
	out5, err5 := getHeaders(invalid2)

	assert.NotNil(err5)
	if err5 != nil {
		assert.Equal("Error parsing headers. Ensure that headers are provided as a JSON of string key-value pairs: json: cannot unmarshal array into Go value of type map[string]string", err5.Error())
	}
	assert.Nil(out5)

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
			testTargetConfig := &HTTPTargetConfig{
				HTTPURL:                 "http://test",
				MessageByteLimit:        1048576,
				RequestByteLimit:        1048576,
				RequestTimeoutInSeconds: 5,
				ContentType:             "application/json",
				DynamicHeaders:          tt.Dynamic,
			}
			testTarget, err := HTTPTargetConfigFunction(testTargetConfig)
			if err != nil {
				t.Fatalf("failed to create test target: " + err.Error())
			}

			out := testTarget.retrieveHeaders(tt.Msg)
			if !reflect.DeepEqual(out, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(out),
					spew.Sdump(tt.Expected))
			}
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

	httpTarget, err := newHTTPTarget("http://something", 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)

	assert.Nil(err)
	assert.NotNil(httpTarget)

	failedHTTPTarget, err1 := newHTTPTarget("something", 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)

	assert.NotNil(err1)
	if err1 != nil {
		assert.Equal("Invalid url for HTTP target: 'something'", err1.Error())
	}
	assert.Nil(failedHTTPTarget)

	failedHTTPTarget2, err2 := newHTTPTarget("", 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
	assert.NotNil(err2)
	if err2 != nil {
		assert.Equal("Invalid url for HTTP target: ''", err2.Error())
	}
	assert.Nil(failedHTTPTarget2)
}

func TestHTTP_Write_Simple(t *testing.T) {
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
			server := createTestServerWithResponseCode(&results, &headers, tt.ResponseCode, "")
			defer server.Close()

			target, err := newHTTPTarget(server.URL, 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
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
			writeResult, err1 := target.Write(messages)

			if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
				assert.Fail("Timed out waiting for acks")
			}

			assert.Nil(err1)
			assert.Equal(int64(25), writeResult.SentCount)
			assert.Equal(int64(0), writeResult.FailedCount)

			assert.Equal(25, len(writeResult.Sent))
			assert.Equal(25, len(results))
			for _, result := range results {
				assert.Equal(`[{"message":"Hello Server!!"}]`, string(result))
			}

			assert.Equal(3, len(writeResult.Invalid)) // invalids went to the right place
			for _, msg := range writeResult.Invalid {
				// Check all invalids have error as expected
				assert.Regexp("Message can't be parsed as valid JSON: .*", msg.GetError().Error())
			}

			assert.Equal(int64(25), ackOps)

			assert.Empty(headers.Get("Request-Timestamp"))
			assert.Empty(headers.Get("Request-Timeout"))
		})
	}
}

func TestHTTP_Write_Batched(t *testing.T) {
	testCases := []struct {
		Name              string
		BatchSize         int
		LastBatchExpected int
	}{
		{Name: "Batches of 20", BatchSize: 20, LastBatchExpected: 20},
		{Name: "Batches of 15", BatchSize: 15, LastBatchExpected: 10},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var results [][]byte
			var headers http.Header
			wg := sync.WaitGroup{}
			server := createTestServerWithResponseCode(&results, &headers, 200, "")
			defer server.Close()

			target, err := newHTTPTarget(server.URL, 5, tt.BatchSize, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
			if err != nil {
				t.Fatal(err)
			}

			var ackOps int64
			ackFunc := func() {
				atomic.AddInt64(&ackOps, 1)

				wg.Done()
			}

			messages := testutil.GetTestMessages(100, `{"message": "Hello Server!!"}`, ackFunc)
			wg.Add(100)
			writeResult, err1 := target.Write(messages)

			if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
				assert.Fail("Timed out waiting for acks")
			}

			assert.Nil(err1)
			assert.Equal(int64(100), writeResult.SentCount)
			assert.Equal(int64(0), writeResult.FailedCount)
			assert.Equal(100, len(writeResult.Sent))
			assert.Equal(math.Ceil(100/float64(tt.BatchSize)), float64(len(results)))
			for i, result := range results {

				var res []json.RawMessage
				err := json.Unmarshal(result, &res)
				if err != nil {
					assert.Fail("Request not an array as expected - got error unmarshalling: " + err.Error())
				}
				// Check the amount f events in the batch is as expected
				if i == len(results)-1 {
					// Check the last batch size
					assert.Equal(tt.LastBatchExpected, len(res))
				} else {
					// Check the others
					assert.Equal(tt.BatchSize, len(res))
				}
				// Iterate and check the data is what we expect
				for _, r := range res {
					assert.Equal(`{"message":"Hello Server!!"}`, string(r))
				}
			}

			assert.Equal(int64(100), ackOps)
		})
	}
}

func TestHTTP_Write_Concurrent(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results)
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
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
			writeResult, err1 := target.Write([]*models.Message{msg})
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

	target, err := newHTTPTarget("http://NonexistentEndpoint", 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)

	writeResult, err1 := target.Write(messages)

	assert.NotNil(err1)
	if err1 != nil {
		assert.Regexp("10 errors occurred:.*", err1.Error())
	}

	assert.Equal(int64(0), writeResult.SentCount)
	assert.Equal(int64(10), writeResult.FailedCount)
	assert.Equal(10, len(writeResult.Failed))
	assert.Empty(writeResult.Sent)
	assert.Empty(writeResult.Oversized)
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
			server := createTestServerWithResponseCode(&results, &headers, tt.ResponseCode, "")
			defer server.Close()
			target, err := newHTTPTarget(server.URL, 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
			if err != nil {
				t.Fatal(err)
			}

			var ackOps int64
			ackFunc := func() {
				atomic.AddInt64(&ackOps, 1)
			}

			messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)
			writeResult, err1 := target.Write(messages)

			assert.NotNil(err1)
			if err1 != nil {
				assert.Regexp("10 errors occurred:.*", err1.Error())
			}

			assert.Equal(int64(0), writeResult.SentCount)
			assert.Equal(int64(10), writeResult.FailedCount)
			assert.Equal(10, len(writeResult.Failed))
			assert.Empty(writeResult.Sent)
			assert.Empty(writeResult.Oversized)
		})
	}
}

func TestHTTP_Write_Oversized(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results)
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 1, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)

		wg.Done()
	}

	messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)
	messages = append(messages, testutil.GetTestMessages(1, testutil.GenRandomString(1048577), ackFunc)...)

	wg.Add(10)
	writeResult, err1 := target.Write(messages)

	if ok := WaitForAcksWithTimeout(2*time.Second, &wg); !ok {
		assert.Fail("Timed out waiting for acks")
	}

	assert.Nil(err1)
	assert.Equal(int64(10), writeResult.SentCount)
	assert.Equal(int64(0), writeResult.FailedCount)
	assert.Equal(10, len(writeResult.Sent))
	assert.Equal(1, len(writeResult.Oversized))
	assert.Equal(10, len(results))
	for _, result := range results {
		assert.Equal(`[{"message":"Hello Server!!"}]`, string(result))
	}

	assert.Equal(int64(10), ackOps)
}

func TestHTTP_Write_EnabledTemplating(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results)
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 5, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", string(`../../integration/http/template`), defaultResponseRules(), false)
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
	writeResult, err1 := target.Write(messages)
	wg.Wait()

	assert.Nil(err1)
	assert.Equal(int64(3), writeResult.SentCount)
	assert.Equal(int64(0), writeResult.FailedCount)
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

	server := createTestServerWithResponseCode(&results, &headers, 400, "Request is invalid. Invalid value for field 'attribute'")
	defer server.Close()

	responseRules := ResponseRules{
		Invalid: []Rule{
			{MatchingHTTPCodes: []int{400, 401}, MatchingBodyPart: "Invalid value for field 'attribute'"},
		},
	}

	target, err := newHTTPTarget(server.URL, 5, 5, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", string(`../../integration/http/template`), &responseRules, false)
	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	writeResult, err1 := target.Write(input)

	assert.Nil(err1)
	assert.Equal(0, len(writeResult.Sent))
	assert.Equal(1, len(writeResult.Invalid))
	assert.Equal("Request is invalid. Invalid value for field 'attribute'", writeResult.Invalid[0].GetError().Error())
}

func TestHTTP_Write_Setup(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 401, "Authentication issue. Invalid token")
	defer server.Close()

	responseRules := ResponseRules{
		SetupError: []Rule{
			{MatchingHTTPCodes: []int{401}, MatchingBodyPart: "Invalid token"},
		},
	}

	target, err := newHTTPTarget(server.URL, 5, 5, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", string(`../../integration/http/template`), &responseRules, false)
	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	writeResult, err := target.Write(input)

	assert.Equal(0, len(writeResult.Sent))
	assert.Equal(0, len(writeResult.Invalid))
	assert.Equal(1, len(writeResult.Failed))

	_, isSetup := err.(models.SetupWriteError)
	assert.True(isSetup)
	assert.Regexp(".*Got setup error, response status: '401 Unauthorized' with error details: 'Invalid token'", err.Error())
}

func TestHTTP_TimeOrientedHeadersEnabled(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	var headers http.Header

	server := createTestServerWithResponseCode(&results, &headers, 200, "ok")
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 5, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, false, "", "", "", "", "", defaultResponseRules(), true)
	if err != nil {
		t.Fatal(err)
	}

	input := []*models.Message{{Data: []byte(`{ "attribute": "value"}`)}}

	beforeRequest := time.Now().UTC().UnixMilli()
	target.Write(input)
	afterRequest := time.Now().UTC().UnixMilli()

	assert.Equal("5000", headers.Get("Request-Timeout"))

	requestTimestamp, _ := strconv.ParseInt(headers.Get("Request-Timestamp"), 10, 64)
	assert.GreaterOrEqual(requestTimestamp, beforeRequest)
	assert.LessOrEqual(requestTimestamp, afterRequest)
}

func TestHTTP_Write_TLS(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Test that https requests work with manually provided certs
	target, err := newHTTPTarget("https://localhost:8999/hello",
		5,
		1,
		1048576,
		1048576,
		"application/json",
		"",
		"",
		"",
		true,
		string(`../../integration/http/localhost.crt`),
		string(`../../integration/http/localhost.key`),
		string(`../../integration/http/rootCA.crt`),
		false,
		false,
		"",
		"",
		"",
		"",
		"",
		defaultResponseRules(), false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, `{"message": "Hello Server!!"}`, ackFunc)

	writeResult, err1 := target.Write(messages)

	assert.Nil(err1)
	assert.Equal(10, len(writeResult.Sent))

	assert.Equal(int64(10), ackOps)

	ngrokAddress := getNgrokAddress() + "/hello"

	// Test that https requests work for different endpoints when different certs are provided manually
	target2, err2 := newHTTPTarget(ngrokAddress,
		5,
		1,
		1048576,
		1048576,
		"application/json",
		"",
		"",
		"",
		true,
		string(`../../integration/http/localhost.crt`),
		string(`../../integration/http/localhost.key`),
		string(`../../integration/http/rootCA.crt`),
		false,
		false,
		"",
		"",
		"",
		"",
		"",
		defaultResponseRules(), false)
	if err2 != nil {
		t.Fatal(err2)
	}

	writeResult2, err3 := target2.Write(messages)

	assert.Nil(err3)
	assert.Equal(10, len(writeResult2.Sent))

	assert.Equal(int64(20), ackOps)

	// Test that https works when certs aren't manually provided

	// Test that https requests work for different endpoints when different certs are provided manually
	target3, err4 := newHTTPTarget(ngrokAddress,
		5,
		1,
		1048576,
		1048576,
		"application/json",
		"",
		"",
		"",
		false,
		"",
		"",
		"",
		false,
		false,
		"",
		"",
		"",
		"",
		"",
		defaultResponseRules(), false)
	if err4 != nil {
		t.Fatal(err4)
	}

	writeResult3, err5 := target3.Write(messages)

	assert.Nil(err5)
	assert.Equal(10, len(writeResult3.Sent))

	assert.Equal(int64(30), ackOps)
}

func TestHTTP_ProvideRequestBody(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{}

	inputMessages := []*models.Message{
		{Data: []byte(`{"key": "value1"}`)},
		{Data: []byte(`{"key": "value2"}`)},
		{Data: []byte(`{"key": "value3"}`)},
		{Data: []byte(`justastring`)},
	}

	templated, success, invalid := target.provideRequestBody(inputMessages)

	assert.Equal(`[{"key":"value1"},{"key":"value2"},{"key":"value3"}]`, string(templated))
	assert.Equal(3, len(success))
	assert.Equal(1, len(invalid))
	assert.Regexp("Message can't be parsed as valid JSON: .*", invalid[0].GetError().Error())
}

func TestHTTP_GroupByHeaders_Disabled(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: false}

	inputMessages := []*models.Message{
		{Data: []byte("value"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 1
		{Data: []byte("value"), HTTPHeaders: map[string]string{"h2": "v2"}}, //group 1
		{Data: []byte("value")}, //group 1
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)

	assert.Len(groupedMessages, 1)
	assert.Equal(inputMessages, groupedMessages[0]) //group 1
}

func TestHTTP_GroupByHeaders_Enabled_SameHeader(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: true}

	inputMessages := []*models.Message{
		{Data: []byte("value1"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 1
		{Data: []byte("value2"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 1
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)

	assert.Len(groupedMessages, 1)
	assert.Equal(inputMessages, groupedMessages[0]) //group 1
}

func TestHTTP_GroupByHeaders_Enabled_NoHeader(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: true}

	inputMessages := []*models.Message{
		{Data: []byte("value1"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 1
		{Data: []byte("value2")}, //group 2
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)
	assert.Len(groupedMessages, 2)

	assert.Contains(groupedMessages, []*models.Message{inputMessages[0]}) //group 1
	assert.Contains(groupedMessages, []*models.Message{inputMessages[1]}) //group 2
}

func TestHTTP_GroupByHeaders_Enabled_DifferentHeaderValue(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: true}

	inputMessages := []*models.Message{
		{Data: []byte("value1"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 1
		{Data: []byte("value2"), HTTPHeaders: map[string]string{"h1": "v2"}}, //group 2
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)
	assert.Len(groupedMessages, 2)

	assert.Contains(groupedMessages, []*models.Message{inputMessages[0]}) //group 1
	assert.Contains(groupedMessages, []*models.Message{inputMessages[1]}) //group 2
}

func TestHTTP_GroupByHeaders_Enabled_DifferentHeaderName(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: true}

	inputMessages := []*models.Message{
		{Data: []byte("value1"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 1
		{Data: []byte("value2"), HTTPHeaders: map[string]string{"h2": "v1"}}, //group 2
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)
	assert.Len(groupedMessages, 2)

	assert.Contains(groupedMessages, []*models.Message{inputMessages[0]}) //group 1
	assert.Contains(groupedMessages, []*models.Message{inputMessages[1]}) //group 2
}

func TestHTTP_GroupByHeaders_Enabled_AdditionalHeader(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: true}

	inputMessages := []*models.Message{
		{Data: []byte("value1"), HTTPHeaders: map[string]string{"h1": "v1"}},                    //group 1
		{Data: []byte("value2"), HTTPHeaders: map[string]string{"h1": "v1", "additonal": "v2"}}, //group 2
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)
	assert.Len(groupedMessages, 2)

	assert.Contains(groupedMessages, []*models.Message{inputMessages[0]}) //group 1
	assert.Contains(groupedMessages, []*models.Message{inputMessages[1]}) //group 2
}

func TestHTTP_GroupByHeaders_Enabled_MultipleGroups(t *testing.T) {
	assert := assert.New(t)
	target := HTTPTarget{dynamicHeaders: true}

	inputMessages := []*models.Message{
		{Data: []byte("value1")}, //group 1
		{Data: []byte("value2")}, //group 1
		{Data: []byte("value3"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 2
		{Data: []byte("value4"), HTTPHeaders: map[string]string{"h1": "v1"}}, //group 2
		{Data: []byte("value5"), HTTPHeaders: map[string]string{"h1": "v2"}}, //group 3
	}

	groupedMessages := target.groupByDynamicHeaders(inputMessages)
	assert.Len(groupedMessages, 3)

	assert.Contains(groupedMessages, []*models.Message{inputMessages[0], inputMessages[1]}) //group 1
	assert.Contains(groupedMessages, []*models.Message{inputMessages[2], inputMessages[3]}) //group 2
	assert.Contains(groupedMessages, []*models.Message{inputMessages[4]})                   //group 3
}

func TestHTTP_Write_GroupedRequests(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	server := createTestServer(&results)
	defer server.Close()

	//dynamicHeaders enabled
	target, err := newHTTPTarget(server.URL, 5, 5, 1048576, 1048576, "application/json", "", "", "", false, "", "", "", true, true, "", "", "", "", "", defaultResponseRules(), false)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}

	ackFunc := func() {
		wg.Done()
	}

	// Add 5 events to match the below
	wg.Add(5)

	inputMessages := []*models.Message{
		{Data: []byte(`{"key": "value1"}`), AckFunc: ackFunc},                                             //group 1
		{Data: []byte(`{"key": "value2"}`), AckFunc: ackFunc},                                             //group 1
		{Data: []byte(`{"key": "value3"}`), AckFunc: ackFunc, HTTPHeaders: map[string]string{"h1": "v1"}}, //group 2
		{Data: []byte(`{"key": "value4"}`), AckFunc: ackFunc, HTTPHeaders: map[string]string{"h1": "v1"}}, //group 2
		{Data: []byte(`{"key": "value5"}`), AckFunc: ackFunc, HTTPHeaders: map[string]string{"h1": "v2"}}, //group 3
	}

	writeResult, err1 := target.Write(inputMessages)

	if ok := WaitForAcksWithTimeout(1*time.Second, &wg); !ok {
		assert.Fail("Timed out waiting for acks")
	}

	assert.Nil(err1)
	assert.Equal(5, len(writeResult.Sent))
	assert.Equal(3, len(results)) // because 3 output groups, 1 request per group

	assert.Contains(results, []byte(`[{"key":"value1"},{"key":"value2"}]`))
	assert.Contains(results, []byte(`[{"key":"value3"},{"key":"value4"}]`))
	assert.Contains(results, []byte(`[{"key":"value5"}]`))
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
	for i := 0; i < 3; i++ { // retry 3 times as this part is flaky
		resp, err = http.DefaultClient.Get("http://localhost:4040/api/tunnels")
		if resp != nil {
			err = nil
			break
		}
	}
	if err != nil {
		panic(err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var ngrokResponse ngrokAPIResponse
	json.Unmarshal(body, &ngrokResponse)

	for _, obj := range ngrokResponse.Tunnels {
		if obj.Proto == "https" {
			return obj.PublicURL
		}
	}
	panic("no ngrok https endpoint found")
}

func defaultResponseRules() *ResponseRules {
	return &ResponseRules{
		Invalid:    []Rule{},
		SetupError: []Rule{},
	}
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
