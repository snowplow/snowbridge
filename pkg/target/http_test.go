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
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/testutil"
)

func createTestServer(results *[][]byte, waitgroup *sync.WaitGroup) *httptest.Server {
	mutex := &sync.Mutex{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		data, err := io.ReadAll(req.Body)
		if err != nil {
			panic(err)
		}
		mutex.Lock()
		*results = append(*results, data)
		mutex.Unlock()
		defer waitgroup.Done()
	}))
}

func TestGetHeaders(t *testing.T) {
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

func TestDecodeMetadata(t *testing.T) {
	testTargetConfig := &HTTPTargetConfig{
		HTTPURL:                 "http://test",
		ByteLimit:               1048576,
		RequestTimeoutInSeconds: 5,
		ContentType:             "application/json",
		MetadataAware:           true,
	}
	testTarget, initerr := HTTPTargetConfigFunction(testTargetConfig)
	if initerr != nil {
		t.Fatalf("failed to create test target")
	}

	testCases := []struct {
		Name     string
		Msg      *models.Message
		Expected *metadataAwareness
		Error    error
	}{
		{
			Name:     "message_has_no_metadata",
			Msg:      &models.Message{},
			Expected: nil,
			Error:    nil,
		},
		{
			Name: "message_has_no_actual_metadata",
			Msg: &models.Message{
				Metadata: &models.Metadata{},
			},
			Expected: nil,
			Error:    nil,
		},
		{
			Name: "actual_metadata_contains_not_relevant_key",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"foo": "bar",
					},
				},
			},
			Expected: &metadataAwareness{},
			Error:    nil,
		},
		{
			Name: "actual_metadata_contains_unused_keys",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"foo": "bar",
						"TargetHTTPHeaders": map[string][]string{
							"newHeader": {"something"},
						},
					},
				},
			},
			Expected: &metadataAwareness{
				TargetHTTPHeaders: map[string][]string{
					"newHeader": {"something"},
				},
			},
			Error: nil,
		},
		{
			Name: "actual_metadata_contains_invalid_TargetHTTPHeaders_1",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"TargetHttpHeaders": "invalid",
					},
				},
			},
			Expected: nil,
			Error:    fmt.Errorf("decoding"),
		},
		{
			Name: "actual_metadata_contains_invalid_TargetHTTPHeaders_2",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"TargetHttpHeaders": map[string]interface{}{
							"newHeader": "test",
						},
					},
				},
			},
			Expected: nil,
			Error:    fmt.Errorf("decoding"),
		},
		{
			Name: "actual_metadata_contains_invalid_TargetHTTPHeaders_3",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"TargetHttpHeaders": map[string]interface{}{
							"newHeader": []int{1, 2},
						},
					},
				},
			},
			Expected: nil,
			Error:    fmt.Errorf("decoding"),
		},
		{
			Name: "happy_path_decoding_1",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"TargetHTTPHeaders": map[string][]interface{}{
							"newHeader": {"test"},
						},
					},
				},
			},
			Expected: &metadataAwareness{
				TargetHTTPHeaders: map[string][]string{
					"newHeader": {"test"},
				},
			},
			Error: nil,
		},
		{
			Name: "happy_path_decoding_2",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"TargetHttpHeaders": map[string][]interface{}{
							"NewHeader": {"test"},
						},
					},
				},
			},
			Expected: &metadataAwareness{
				TargetHTTPHeaders: map[string][]string{
					"NewHeader": {"test"},
				},
			},
			Error: nil,
		},
		{
			Name: "happy_path_decoding_3",
			Msg: &models.Message{
				Metadata: &models.Metadata{
					Actual: map[string]interface{}{
						"targetHTTPHeaders": map[string][]interface{}{
							"newHeader": {"test"},
						},
					},
				},
			},
			Expected: &metadataAwareness{
				TargetHTTPHeaders: map[string][]string{
					"newHeader": {"test"},
				},
			},
			Error: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			out, err := testTarget.decodeMetadata(tt.Msg)
			if err == nil && tt.Error != nil {
				t.Fatalf("Got no error when it was expected to get error: %s", tt.Error.Error())
			}

			if err != nil {
				if tt.Error == nil {
					t.Fatalf("got unexpected error: %s", err.Error())
				}

				if !strings.Contains(err.Error(), tt.Error.Error()) {
					t.Fatalf("Actual error:\n%s\n does not contain Expected error:\n%s",
						err.Error(),
						tt.Error.Error())
				}
			}

			if !reflect.DeepEqual(out, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(out),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestAddHeadersToRequest(t *testing.T) {
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

func TestAddHeadersToRequest_WithMetadata(t *testing.T) {
	testCases := []struct {
		Name           string
		ConfigHeaders  map[string]string
		Meta           *metadataAwareness
		ExpectedHeader http.Header
	}{
		{
			Name:           "meta_nil_config_nil",
			ConfigHeaders:  nil,
			Meta:           nil,
			ExpectedHeader: http.Header{},
		},
		{
			Name:           "meta_empty_config_nil",
			ConfigHeaders:  nil,
			Meta:           &metadataAwareness{},
			ExpectedHeader: http.Header{},
		},
		{
			Name: "meta_nil_plus_config",
			ConfigHeaders: map[string]string{
				"Max Forwards": "10",
			},
			Meta: nil,
			ExpectedHeader: http.Header{
				"Max Forwards": {"10"},
			},
		},
		{
			Name: "meta_empty_plus_config",
			ConfigHeaders: map[string]string{
				"Max Forwards": "10",
			},
			Meta: &metadataAwareness{},
			ExpectedHeader: http.Header{
				"Max Forwards": {"10"},
			},
		},
		{
			Name: "meta_header_plus_config",
			ConfigHeaders: map[string]string{
				"Max Forwards": "10",
			},
			Meta: &metadataAwareness{
				TargetHTTPHeaders: map[string][]string{
					"Content-Length": {"0", "1"},
				},
			},
			ExpectedHeader: http.Header{
				"Max Forwards":   {"10"},
				"Content-Length": {"0", "1"},
			},
		},
		{
			Name: "meta_headers_same_key_with_config",
			ConfigHeaders: map[string]string{
				"Max Forwards":   "10",
				"Content-Length": "0",
			},
			Meta: &metadataAwareness{
				TargetHTTPHeaders: map[string][]string{
					"Content-Length": {"1", "2"},
					"Test-Header":    {"test"},
					"Empty":          {},
				},
			},
			ExpectedHeader: http.Header{
				"Max Forwards":   {"10"},
				"Content-Length": {"0", "1", "2"},
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

			addHeadersToRequest(req, tt.ConfigHeaders, tt.Meta)
			assert.Equal(tt.ExpectedHeader, req.Header)
		})
	}
}

func TestNewHTTPTarget(t *testing.T) {
	assert := assert.New(t)

	httpTarget, err := newHTTPTarget("http://something", 5, 1048576, "application/json", "", "", "", "", "", "", true, false)

	assert.Nil(err)
	assert.NotNil(httpTarget)

	failedHTTPTarget, err1 := newHTTPTarget("something", 5, 1048576, "application/json", "", "", "", "", "", "", true, false)

	assert.NotNil(err1)
	if err1 != nil {
		assert.Equal("Invalid url for HTTP target: 'something'", err1.Error())
	}
	assert.Nil(failedHTTPTarget)

	failedHTTPTarget2, err2 := newHTTPTarget("", 5, 1048576, "application/json", "", "", "", "", "", "", true, false)
	assert.NotNil(err2)
	if err2 != nil {
		assert.Equal("Invalid url for HTTP target: ''", err2.Error())
	}
	assert.Nil(failedHTTPTarget2)
}

func TestHttpWrite_Simple(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results, &wg)
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true, false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(501, "Hello Server!!", ackFunc)
	wg.Add(501)
	writeResult, err1 := target.Write(messages)

	wg.Wait()

	assert.Nil(err1)
	assert.Equal(501, len(writeResult.Sent))
	assert.Equal(501, len(results))
	for _, result := range results {
		assert.Equal("Hello Server!!", string(result))
	}

	assert.Equal(int64(501), ackOps)
}

func TestHttpWrite_Concurrent(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results, &wg)
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true, false)
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

	messages := testutil.GetTestMessages(10, "Hello Server!!", ackFunc)

	for _, message := range messages {
		wg.Add(2) // Both acking and returning results from server can have race conditions, so we add both to the waitgroup.
		go func(msg *models.Message) {
			writeResult, err1 := target.Write([]*models.Message{msg})
			assert.Nil(err1)
			assert.Equal(1, len(writeResult.Sent))
		}(message)
	}

	wg.Wait()

	assert.Equal(10, len(results))
	for _, result := range results {
		assert.Equal("Hello Server!!", string(result))
	}

	assert.Equal(int64(10), ackOps)
}

func TestHttpWrite_Failure(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results, &wg)
	defer server.Close()

	target, err := newHTTPTarget("http://NonexistentEndpoint", 5, 1048576, "application/json", "", "", "", "", "", "", true, false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Server!!", ackFunc)

	writeResult, err1 := target.Write(messages)

	assert.NotNil(err1)
	if err1 != nil {
		assert.Regexp("Error sending http requests: 10 errors occurred:.*", err1.Error())
	}

	assert.Equal(10, len(writeResult.Failed))
	assert.Nil(writeResult.Sent)
	assert.Nil(writeResult.Oversized)
}

func TestHttpWrite_Oversized(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results, &wg)
	defer server.Close()

	target, err := newHTTPTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true, false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Server!!", ackFunc)
	messages = append(messages, testutil.GetTestMessages(1, testutil.GenRandomString(1048577), ackFunc)...)

	wg.Add(10)
	writeResult, err1 := target.Write(messages)

	wg.Wait()

	assert.Nil(err1)
	assert.Equal(10, len(writeResult.Sent))
	assert.Equal(1, len(writeResult.Oversized))
	assert.Equal(10, len(results))
	for _, result := range results {
		assert.Equal("Hello Server!!", string(result))
	}

	assert.Equal(int64(10), ackOps)
}

// Steps to create certs manually:

// openssl genrsa -out rootCA.key 4096
// openssl req -x509 -new -key rootCA.key -days 3650 -out rootCA.crt -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost"

// openssl genrsa -out localhost.key 2048
// openssl req -new -key localhost.key -out localhost.csr -subj "/CN=localhost" -addext "subjectAltName = DNS:localhost"
// openssl x509 -req -in localhost.csr -CA rootCA.crt -CAkey rootCA.key -CAcreateserial -days 365 -out localhost.crt

func TestHttpWrite_TLS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Test that https requests work with manually provided certs
	target, err := newHTTPTarget("https://localhost:8999/hello",
		5,
		1048576,
		"application/json",
		"",
		"",
		"",
		string(`../../integration/http/localhost.crt`),
		string(`../../integration/http/localhost.key`),
		string(`../../integration/http/rootCA.crt`),
		false,
		false)
	if err != nil {
		t.Fatal(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Server!!", ackFunc)

	writeResult, err1 := target.Write(messages)

	assert.Nil(err1)
	assert.Equal(10, len(writeResult.Sent))

	assert.Equal(int64(10), ackOps)

	ngrokAddress := getNgrokAddress() + "/hello"

	// Test that https requests work for different endpoints when different certs are provided manually
	target2, err2 := newHTTPTarget(ngrokAddress,
		5,
		1048576,
		"application/json",
		"",
		"",
		"",
		string(`../../integration/http/localhost.crt`),
		string(`../../integration/http/localhost.key`),
		string(`../../integration/http/rootCA.crt`),
		false,
		false)
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
		1048576,
		"application/json",
		"",
		"",
		"",
		"",
		"",
		"",
		false,
		false)
	if err4 != nil {
		t.Fatal(err4)
	}

	writeResult3, err5 := target3.Write(messages)

	assert.Nil(err5)
	assert.Equal(10, len(writeResult3.Sent))

	assert.Equal(int64(30), ackOps)
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
