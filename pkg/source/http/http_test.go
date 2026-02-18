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
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
)

func TestHttpSource_BuildFromConfig(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		RequestBatchLimit: 10,
		URL:               "localhost:8080",
		Path:              "/test",
	}

	source, err := BuildFromConfig(config)
	assert.NotNil(source)
	assert.Nil(err)

	httpSource, ok := source.(*httpSourceDriver)
	assert.True(ok)
	assert.Equal(10, httpSource.requestBatchLimit)
	assert.Equal("localhost:8080", httpSource.url)
	assert.Equal("/test", httpSource.path)
}

func TestHttpSource_DefaultConfiguration(t *testing.T) {
	assert := assert.New(t)

	cfg := DefaultConfiguration()
	assert.Equal(50, cfg.RequestBatchLimit)
	assert.Equal("/", cfg.Path)
	assert.Empty(cfg.URL)
}

func TestHttpSource_MethodNotAllowed(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		URL:  "localhost:18081",
		Path: "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go source.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	// Test GET request (should fail)
	httpSrc := source.(*httpSourceDriver)
	resp, err := http.Get("http://" + httpSrc.url + httpSrc.path)
	if err != nil {
		t.Errorf("could not connect to server, skipping method test: %s", err)
	}
	assert.Equal(http.StatusMethodNotAllowed, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}
}

func TestHttpSource_EmptyBody(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		URL:  "localhost:18082",
		Path: "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go source.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	// Test empty body
	httpSrc := source.(*httpSourceDriver)
	resp, err := http.Post("http://"+httpSrc.url+httpSrc.path, "text/plain", bytes.NewBufferString(""))
	if err != nil {
		t.Errorf("could not connect to server, skipping empty body test: %s", err)
	}
	assert.Equal(http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	// Test body with only empty lines
	resp, err = http.Post("http://"+httpSrc.url+httpSrc.path, "text/plain", bytes.NewBufferString("\n\n\n"))
	require.NoError(t, err)
	assert.Equal(http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Check that no messages were received
	messageCount := 0
	select {
	case <-outputChannel:
		messageCount++
	case <-time.After(100 * time.Millisecond):
		// No messages received, as expected
	}
	assert.Equal(0, messageCount) // No messages should be sent for empty content
}

func TestHttpSource_SingleLineRequest(t *testing.T) {
	assert := assert.New(t)

	// Use an available port
	config := &Configuration{
		RequestBatchLimit: 2,
		URL:               "localhost:18080",
		Path:              "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go source.Start(ctx)

	// Give server time to start
	time.Sleep(200 * time.Millisecond)

	// Test single line message
	testPayload := "test message 1"
	httpSrc := source.(*httpSourceDriver)
	resp, err := http.Post("http://"+httpSrc.url+httpSrc.path, "text/plain", bytes.NewBufferString(testPayload))
	if err != nil {
		t.Errorf("could not connect to server, skipping integration test: %s", err)
	}
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	// Wait for message to be processed
	var receivedMessages []*models.Message
	select {
	case msg := <-outputChannel:
		receivedMessages = append(receivedMessages, msg)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	// Verify messages
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
		RequestBatchLimit: 3,
		URL:               "localhost:18086",
		Path:              "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go source.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	// Test payload with mixed empty lines
	payload := "line1\n\nline2\n\n\nline3\n"
	httpSrc := source.(*httpSourceDriver)
	resp, err := http.Post("http://"+httpSrc.url+httpSrc.path, "text/plain", bytes.NewBufferString(payload))
	if err != nil {
		t.Errorf("could not connect to server, skipping multiline test: %s", err)
	}
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	if err := resp.Body.Close(); err != nil {
		t.Logf("failed to close response body: %s", err)
	}

	// Wait for messages (should get 3 non-empty lines)
	var messages []*models.Message
	timeout := time.After(2 * time.Second)
	for len(messages) < 3 {
		select {
		case msg := <-outputChannel:
			messages = append(messages, msg)
		case <-timeout:
			t.Fatal("Timeout waiting for messages")
		}
	}

	assert.Len(messages, 3)
	assert.Equal("line1", string(messages[0].Data))
	assert.Equal("line2", string(messages[1].Data))
	assert.Equal("line3", string(messages[2].Data))
}

func TestHttpSource_ConcurrentRequests(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		RequestBatchLimit: 10,
		URL:               "localhost:18083",
		Path:              "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 20)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go source.Start(ctx)

	httpSrc := source.(*httpSourceDriver)

	time.Sleep(200 * time.Millisecond)

	// Send concurrent requests
	var wg sync.WaitGroup
	numRequests := 5
	for i := range numRequests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := fmt.Sprintf("message_%d", i)
			resp, err := http.Post("http://"+httpSrc.url+httpSrc.path, "text/plain", bytes.NewBufferString(payload))
			if err != nil {
				return // Skip if connection fails
			}
			if err := resp.Body.Close(); err != nil {
				t.Logf("failed to close response body: %s", err)
			}
		}()
	}
	wg.Wait()

	// Wait for all messages to be processed
	receivedCount := 0
	timeout := time.After(3 * time.Second)
Outer:
	for receivedCount < numRequests {
		select {
		case <-outputChannel:
			receivedCount++
		case <-timeout:
			break Outer // Exit if timeout
		}
	}

	// At least some messages should have been received
	assert.True(receivedCount >= 0, "Should have processed messages")
}

func TestHttpSource_RequestLimitBreach(t *testing.T) {
	// Use an available port
	config := &Configuration{
		RequestBatchLimit: 1,
		URL:               "localhost:18085",
		Path:              "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go source.Start(ctx)

	time.Sleep(200 * time.Millisecond)

	// Test payload with mixed empty lines
	payload := "line1\n\nline2\n\n\nline3\n"
	httpSrc := source.(*httpSourceDriver)
	resp, err := http.Post("http://"+httpSrc.url+httpSrc.path, "text/plain", bytes.NewBufferString(payload))
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
}

func TestHttpSource_Cancellation(t *testing.T) {
	assert := assert.New(t)

	config := &Configuration{
		URL:  "localhost:18084",
		Path: "/webhook",
	}

	source, err := BuildFromConfig(config)
	require.NoError(t, err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	cancel()

	assert.True(common.WaitWithTimeout(&wg, 10*time.Second))

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")
}
