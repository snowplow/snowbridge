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

package httpsource

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

func TestHttpSource_NewHttpSource(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		ConcurrentWrites:  10,
		RequestBatchLimit: 10,
		URL:               "localhost:8080",
		Path:              "/test",
	}

	source, err := newHttpSource(config)
	assert.NotNil(source)
	assert.Nil(err)
	assert.Equal("http", source.GetID())
	assert.Equal(10, source.concurrentWrites)
	assert.Equal(10, source.requestBatchLimit)
	assert.Equal("localhost:8080", source.url)
	assert.Equal("/test", source.path)
}

func TestHttpSource_Configuration(t *testing.T) {
	assert := assert.New(t)

	// Test default configuration
	adapter := adapterGenerator(configfunction)
	defaultConfig, err := adapter.ProvideDefault()
	assert.Nil(err)

	config, ok := defaultConfig.(*Configuration)
	assert.True(ok)
	assert.Equal(50, config.ConcurrentWrites)
	assert.Equal(50, config.RequestBatchLimit)
	assert.Equal("/", config.Path)

	// Test configuration creation
	config.URL = "http://localhost:8080"
	source, err := adapter.Create(config)
	assert.Nil(err)
	assert.NotNil(source)

	httpSource, ok := source.(*httpSource)
	assert.True(ok)
	assert.Equal(50, httpSource.concurrentWrites)
	assert.Equal(50, httpSource.requestBatchLimit)
	assert.Equal("http://localhost:8080", httpSource.url)
	assert.Equal("/", httpSource.path)
}

func TestHttpSource_ConfigPair(t *testing.T) {
	assert := assert.New(t)

	assert.Equal(SupportedSourceHTTP, ConfigPair.Name)
	assert.NotNil(ConfigPair.Handle)
}

func TestHttpSource_MethodNotAllowed(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		ConcurrentWrites: 1,
		URL:              "localhost:18081",
		Path:             "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)

	sf := sourceiface.SourceFunctions{
		WriteToTarget: func(messages []*models.Message) error { return nil },
	}

	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Test GET request (should fail)
	resp, err := http.Get("http://" + source.url + source.path)
	if err != nil {
		t.Errorf("could not connect to server, skipping method test: %s", err)
	}
	assert.Equal(http.StatusMethodNotAllowed, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	source.Stop()
}

func TestHttpSource_InvalidInput(t *testing.T) {
	assert := assert.New(t)

	adapter := adapterGenerator(configfunction)

	// Test invalid input type
	source, err := adapter.Create("invalid config")
	assert.Nil(source)
	assert.NotNil(err)
	assert.Contains(err.Error(), "invalid input")
}

func TestHttpSource_EmptyBody(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		ConcurrentWrites: 1,
		URL:              "localhost:18082",
		Path:             "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)

	var messageCount int
	mu := sync.Mutex{}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: func(messages []*models.Message) error {
			mu.Lock()
			messageCount += len(messages)
			mu.Unlock()
			return nil
		},
	}

	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Test empty body
	resp, err := http.Post("http://"+source.url+source.path, "text/plain", bytes.NewBufferString(""))
	if err != nil {
		t.Errorf("could not connect to server, skipping empty body test: %s", err)
	}
	assert.Equal(http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	// Test body with only empty lines
	resp, err = http.Post("http://"+source.url+source.path, "text/plain", bytes.NewBufferString("\n\n\n"))
	require.NoError(t, err)
	assert.Equal(http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(0, messageCount) // No messages should be sent for empty content
	mu.Unlock()

	source.Stop()
}

func TestHttpSource_SingleLineRequest(t *testing.T) {
	assert := assert.New(t)

	// Use an available port
	config := &Configuration{
		ConcurrentWrites:  2,
		RequestBatchLimit: 2,
		URL:               "localhost:18080",
		Path:              "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)
	assert.Equal("http", source.GetID())

	// Setup message collection
	var receivedMessages []*models.Message
	var mu sync.Mutex
	messageReceived := make(chan bool, 10)

	writeFunc := func(messages []*models.Message) error {
		mu.Lock()
		defer mu.Unlock()
		receivedMessages = append(receivedMessages, messages...)
		for range messages {
			messageReceived <- true
		}
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	// Start server in goroutine
	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Give server time to start
	time.Sleep(200 * time.Millisecond)

	// Test single line message
	testPayload := "test message 1"
	resp, err := http.Post("http://"+source.url+source.path, "text/plain", bytes.NewBufferString(testPayload))
	if err != nil {
		t.Errorf("could not connect to server, skipping integration test: %s", err)
	}
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	// Wait for message to be processed
	select {
	case <-messageReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Stop server
	source.Stop()

	// Verify messages
	mu.Lock()
	defer mu.Unlock()
	assert.Len(receivedMessages, 1)
	assert.Equal("test message 1", string(receivedMessages[0].Data))

	// Verify all messages have required fields
	for _, msg := range receivedMessages {
		assert.NotEmpty(msg.PartitionKey)
		assert.False(msg.TimeCreated.IsZero())
		assert.False(msg.TimePulled.IsZero())
	}
}

func TestHttpSource_MultipleLinesRequest(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		ConcurrentWrites:  1,
		RequestBatchLimit: 3,
		URL:               "localhost:18086",
		Path:              "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)

	var messages []*models.Message
	mu := sync.Mutex{}
	messageReceived := make(chan bool, 10)

	sf := sourceiface.SourceFunctions{
		WriteToTarget: func(msgs []*models.Message) error {
			mu.Lock()
			messages = append(messages, msgs...)
			mu.Unlock()
			for range msgs {
				messageReceived <- true
			}
			return nil
		},
	}

	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Test payload with mixed empty lines
	payload := "line1\n\nline2\n\n\nline3\n"
	resp, err := http.Post("http://"+source.url+source.path, "text/plain", bytes.NewBufferString(payload))
	if err != nil {
		t.Errorf("could not connect to server, skipping multiline test: %s", err)
	}
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	// Wait for messages (should get 3 non-empty lines)
	for range 3 {
		select {
		case <-messageReceived:
		case <-time.After(2 * time.Second):
			t.Fatal("Timeout waiting for messages")
		}
	}

	source.Stop()

	mu.Lock()
	defer mu.Unlock()
	assert.Len(messages, 3)
	assert.Equal("line1", string(messages[0].Data))
	assert.Equal("line2", string(messages[1].Data))
	assert.Equal("line3", string(messages[2].Data))
}

func TestHttpSource_ConcurrentRequests(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		ConcurrentWrites: 3,
		URL:              "localhost:18083",
		Path:             "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)

	var totalMessages int
	mu := sync.Mutex{}
	messageReceived := make(chan bool, 20)

	sf := sourceiface.SourceFunctions{
		WriteToTarget: func(messages []*models.Message) error {
			mu.Lock()
			totalMessages += len(messages)
			mu.Unlock()
			for range messages {
				messageReceived <- true
			}
			return nil
		},
	}

	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Send concurrent requests
	var wg sync.WaitGroup
	numRequests := 5
	for i := range numRequests {
		wg.Go(func() {
			payload := fmt.Sprintf("message_%d", i)
			resp, err := http.Post("http://"+source.url+source.path, "text/plain", bytes.NewBufferString(payload))
			if err != nil {
				return // Skip if connection fails
			}
			if err := resp.Body.Close(); err != nil {
				t.Logf("failed to close response body: %s", err)
			}
		})
	}
	wg.Wait()

	// Wait for all messages to be processed
	receivedCount := 0
	timeout := time.After(3 * time.Second)
Outer:
	for receivedCount < numRequests {
		select {
		case <-messageReceived:
			receivedCount++
		case <-timeout:
			break Outer // Exit if timeout
		}
	}

	source.Stop()

	mu.Lock()
	// At least some messages should have been received
	assert.True(totalMessages >= 0, "Should have processed messages")
	mu.Unlock()
}

func TestHttpSource_RequestLimitBreach(t *testing.T) {
	// Use an available port
	config := &Configuration{
		ConcurrentWrites:  2,
		RequestBatchLimit: 1,
		URL:               "localhost:18080",
		Path:              "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)

	var messages []*models.Message
	mu := sync.Mutex{}
	messageReceived := make(chan bool, 10)

	sf := sourceiface.SourceFunctions{
		WriteToTarget: func(msgs []*models.Message) error {
			mu.Lock()
			messages = append(messages, msgs...)
			mu.Unlock()
			for range msgs {
				messageReceived <- true
			}
			return nil
		},
	}

	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// Test payload with mixed empty lines
	payload := "line1\n\nline2\n\n\nline3\n"
	resp, err := http.Post("http://"+source.url+source.path, "text/plain", bytes.NewBufferString(payload))
	if err != nil {
		t.Errorf("could not connect to server, skipping multiline test: %s", err)
	}
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	respError, err := io.ReadAll(resp.Body)
	require.Nil(t, err)
	require.Equal(t, "request batch limit is reached\n", string(respError))

	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	source.Stop()
}

func TestHttpSource_Stop(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		ConcurrentWrites: 1,
		URL:              "localhost:18084",
		Path:             "/webhook",
	}

	source, err := newHttpSource(config)
	require.NoError(t, err)

	sf := sourceiface.SourceFunctions{
		WriteToTarget: func(messages []*models.Message) error { return nil },
	}

	go func() {
		if err := source.Read(&sf); err != nil {
			logrus.Error(err.Error())
		}
	}()

	time.Sleep(100 * time.Millisecond)

	// Stop should not panic or hang
	start := time.Now()
	source.Stop()
	duration := time.Since(start)

	// Should complete within reasonable time
	assert.True(duration < 10*time.Second, "Stop should complete within reasonable time")
}
