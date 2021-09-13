// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func createTestServer(results *[][]byte, waitgroup *sync.WaitGroup) *httptest.Server {
	mutex := &sync.Mutex{}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		defer req.Body.Close()
		data, err := ioutil.ReadAll(req.Body)
		if err != nil {
			panic(err) // If we hit this error, something went wrong with the test setup, so panic
		}
		mutex.Lock()
		*results = append(*results, data)
		mutex.Unlock()
		defer waitgroup.Done()
	}))
}

func TestGetHeaders(t *testing.T) {
	assert := assert.New(t)
	valid1 := "Max Forwards::10,,Accept-Language::en-US,,Accept-Datetime::Thu, 31 May 2007 20:35:00 GMT"

	expected1 := [][]string{{"Max Forwards", "10"}, {"Accept-Language", "en-US"}, {"Accept-Datetime", "Thu, 31 May 2007 20:35:00 GMT"}}

	out1, err1 := getHeaders(valid1)

	assert.Nil(err1)
	assert.Equal(expected1, out1)

	valid2 := "Max Forwards::10"
	expected2 := [][]string{{"Max Forwards", "10"}}

	out2, err2 := getHeaders(valid2)

	assert.Nil(err2)
	assert.Equal(expected2, out2)

	invalid1 := "name1::content1::name2"
	out3, err3 := getHeaders(invalid1)

	assert.Equal("Error parsting headers. Ensure that headers are provided in `{name1}::{value1},,{name2}::{value2}` format", err3.Error())
	assert.Nil(out3)

	invalid2 := "name1,,content1,,name2,,content2"
	out4, err4 := getHeaders(invalid2)
	assert.Equal("Error parsting headers. Ensure that headers are provided in `{name1}::{value1},,{name2}::{value2}` format", err4.Error())
	assert.Nil(out4)

	invalid3 := "name1:content1"
	out5, err5 := getHeaders(invalid3)
	assert.Equal("Error parsting headers. Ensure that headers are provided in `{name1}::{value1},,{name2}::{value2}` format", err5.Error())
	assert.Nil(out5)
}

func TestAddHeadersToRequest(t *testing.T) {
	assert := assert.New(t)

	req, err := http.NewRequest("POST", "abc", bytes.NewBuffer([]byte("def")))
	if err != nil {
		panic(err)
	}
	headersToAdd := [][]string{{"Max Forwards", "10"}, {"Accept-Language", "en-US"}, {"Accept-Language", "en-IE"}, {"Accept-Datetime", "Thu, 31 May 2007 20:35:00 GMT"}}

	expectedHeaders := http.Header{
		"Max Forwards":    []string{"10"},
		"Accept-Language": []string{"en-US", "en-IE"},
		"Accept-Datetime": []string{"Thu, 31 May 2007 20:35:00 GMT"},
	}

	addHeadersToRequest(req, headersToAdd)
	assert.Equal(expectedHeaders, req.Header)

	req2, err2 := http.NewRequest("POST", "abc", bytes.NewBuffer([]byte("def")))
	if err2 != nil {
		panic(err2)
	}
	var noHeadersToAdd [][]string
	noHeadersExpected := http.Header{}

	addHeadersToRequest(req2, noHeadersToAdd)

	assert.Equal(noHeadersExpected, req2.Header)
}

func TestNewHttpTarget(t *testing.T) {
	assert := assert.New(t)

	httpTarget, err := NewHttpTarget("http://something", 5, 1048576, "application/json", "", "", "", "", "", "", true)

	assert.Nil(err)
	assert.NotNil(httpTarget)

	failedHttpTarget, err1 := NewHttpTarget("something", 5, 1048576, "application/json", "", "", "", "", "", "", true)

	assert.Equal("Invalid url for Http target: 'something'", err1.Error())
	assert.Nil(failedHttpTarget)

	failedHttpTarget2, err2 := NewHttpTarget("", 5, 1048576, "application/json", "", "", "", "", "", "", true)
	assert.Equal("Invalid url for Http target: ''", err2.Error())
	assert.Nil(failedHttpTarget2)
}

func TestHttpWrite_Simple(t *testing.T) {
	assert := assert.New(t)

	var results [][]byte
	wg := sync.WaitGroup{}
	server := createTestServer(&results, &wg)
	defer server.Close()

	target, err := NewHttpTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true)
	if err != nil {
		panic(err)
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

	target, err := NewHttpTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true)
	if err != nil {
		panic(err)
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

	target, err := NewHttpTarget("http://NonexistentEndpoint", 5, 1048576, "application/json", "", "", "", "", "", "", true)
	if err != nil {
		panic(err)
	}

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, "Hello Server!!", ackFunc)

	writeResult, err1 := target.Write(messages)

	assert.NotNil(err1)

	assert.Regexp("Error sending http request: 10 errors occurred:.*", err1.Error())

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

	target, err := NewHttpTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true)
	if err != nil {
		panic(err)
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

	target, err := NewHttpTarget("https://localhost:8999/hello",
		5,
		1048576,
		"application/json",
		"",
		"",
		"",
		os.Getenv("CERT_DIR")+"/localhost.crt",
		os.Getenv("CERT_DIR")+"/localhost.key",
		os.Getenv("CERT_DIR")+"/rootCA.crt",
		false)
	if err != nil {
		panic(err)
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
}
