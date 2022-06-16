// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
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

	addHeadersToRequest(req, headersToAdd)
	assert.Equal(expectedHeaders, req.Header)

	req2, err2 := http.NewRequest("POST", "abc", bytes.NewBuffer([]byte("def")))
	if err2 != nil {
		t.Fatal(err2)
	}
	var noHeadersToAdd map[string]string
	noHeadersExpected := http.Header{}

	addHeadersToRequest(req2, noHeadersToAdd)

	assert.Equal(noHeadersExpected, req2.Header)
}

func TestNewHTTPTarget(t *testing.T) {
	assert := assert.New(t)

	httpTarget, err := newHTTPTarget("http://something", 5, 1048576, "application/json", "", "", "", "", "", "", true)

	assert.Nil(err)
	assert.NotNil(httpTarget)

	failedHTTPTarget, err1 := newHTTPTarget("something", 5, 1048576, "application/json", "", "", "", "", "", "", true)

	assert.NotNil(err1)
	if err1 != nil {
		assert.Equal("Invalid url for HTTP target: 'something'", err1.Error())
	}
	assert.Nil(failedHTTPTarget)

	failedHTTPTarget2, err2 := newHTTPTarget("", 5, 1048576, "application/json", "", "", "", "", "", "", true)
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

	target, err := newHTTPTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true)
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

	target, err := newHTTPTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true)
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

	target, err := newHTTPTarget("http://NonexistentEndpoint", 5, 1048576, "application/json", "", "", "", "", "", "", true)
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
		assert.Regexp("Error sending http request: 10 errors occurred:.*", err1.Error())
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

	target, err := newHTTPTarget(server.URL, 5, 1048576, "application/json", "", "", "", "", "", "", true)
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

	defer os.RemoveAll(`tmp_replicator`)

	crt, err := os.ReadFile(`../../integration/http/localhost.crt`)
	if err != nil {
		return
	}
	encodedCrt := base64.StdEncoding.EncodeToString(crt)
	key, err := os.ReadFile(`../../integration/http/localhost.key`)
	if err != nil {
		return
	}
	encodedKey := base64.StdEncoding.EncodeToString(key)
	ca, err := os.ReadFile(`../../integration/http/rootCA.crt`)
	if err != nil {
		return
	}
	encodedCa := base64.StdEncoding.EncodeToString(ca)

	// Test that https requests work with manually provided certs
	target, err := newHTTPTarget("https://localhost:8999/hello",
		5,
		1048576,
		"application/json",
		"",
		"",
		"",
		string(encodedCrt),
		string(encodedKey),
		string(encodedCa),
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

	os.RemoveAll(`tmp_replicator`)

	// Test that https requests work for different endpoints when different certs are provided manually
	target2, err2 := newHTTPTarget(ngrokAddress,
		5,
		1048576,
		"application/json",
		"",
		"",
		"",
		string(encodedCrt),
		string(encodedKey),
		string(encodedCa),
		false)
	if err2 != nil {
		os.RemoveAll(`tmp_replicator`)
		t.Fatal(err2)
	}

	writeResult2, err3 := target2.Write(messages)

	assert.Nil(err3)
	assert.Equal(10, len(writeResult2.Sent))

	assert.Equal(int64(20), ackOps)

	os.RemoveAll(`tmp_replicator`)

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
		false)
	if err4 != nil {
		os.RemoveAll(`tmp_replicator`)
		t.Fatal(err4)
	}

	writeResult3, err5 := target3.Write(messages)

	assert.Nil(err5)
	assert.Equal(10, len(writeResult3.Sent))

	assert.Equal(int64(30), ackOps)
	os.RemoveAll(`tmp_replicator`)
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
	resp, err := http.DefaultClient.Get("http://localhost:4040/api/tunnels")
	if err != nil {
		panic(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
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
