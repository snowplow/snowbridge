/**
 * Copyright (c) 2025-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package monitoring

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- Test WebhookSender

type TestWebhookSender struct {
	onDo func(b *http.Request) (*http.Response, error)
}

func (s *TestWebhookSender) Do(b *http.Request) (*http.Response, error) {
	return s.onDo(b)
}

// --- Tests

func TestWebhookMonitoringTargetWrite(t *testing.T) {
	assert := assert.New(t)

	expectedHeartbeatRequest := struct {
		Method string
		URL    string
		Body   WebhookEvent
	}{
		Method: "POST",
		URL:    "https://test.webhook.com",
		Body: WebhookEvent{
			Schema: "iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0",
			Data: WebhookData{
				AppName:    "snowbridge",
				AppVersion: "3.4.0",
			},
		},
	}

	expectedAlertRequest := struct {
		Method string
		URL    string
		Body   WebhookEvent
	}{
		Method: "POST",
		URL:    "https://test.webhook.com",
		Body: WebhookEvent{
			Schema: "iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0",
			Data: WebhookData{
				AppName:    "snowbridge",
				AppVersion: "3.4.0",
				Message:    "failed to connect to target API",
			},
		},
	}

	t.Run("simple heartbeats", func(t *testing.T) {
		counter := 0

		onDo := func(b *http.Request) (*http.Response, error) {
			assert.NotNil(b)

			var actualBody WebhookEvent
			if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
				t.Fatalf("not expecting error: %s", err)
			}

			assert.Equal(expectedHeartbeatRequest.Body, actualBody)
			assert.Equal(expectedHeartbeatRequest.Method, b.Method)
			assert.Equal(expectedHeartbeatRequest.URL, b.URL.String())

			counter++
			return nil, nil
		}

		sr := TestWebhookSender{onDo: onDo}

		webhook := NewWebhookMonitoring("snowbridge", "3.4.0", &sr, "https://test.webhook.com", nil, time.Second, nil)
		assert.NotNil(webhook)
		webhook.Start()

		time.Sleep(2200 * time.Millisecond)
		assert.Equal(counter, 2)

		webhook.Stop()
	})

	t.Run("continuous alerts with backoff", func(t *testing.T) {
		counter := 0

		onDo := func(b *http.Request) (*http.Response, error) {
			assert.NotNil(b)

			var actualBody WebhookEvent
			if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
				t.Fatalf("not expecting error: %s", err)
			}

			assert.Equal(expectedAlertRequest.Body, actualBody)
			assert.Equal(expectedAlertRequest.Method, b.Method)
			assert.Equal(expectedAlertRequest.URL, b.URL.String())

			counter++
			return nil, nil
		}

		sr := TestWebhookSender{onDo: onDo}
		alertChan := make(chan error, 1)

		webhook := NewWebhookMonitoring("snowbridge", "3.4.0", &sr, "https://test.webhook.com", nil, 500*time.Millisecond, alertChan)
		assert.NotNil(webhook)

		webhook.Start()

		// Send initial error
		alertChan <- fmt.Errorf("failed to connect to target API")

		// Expect first alert to be sent immediately
		time.Sleep(50 * time.Millisecond)
		assert.Equal(counter, 1)

		// Wait for first backoff period to pass (500ms)
		time.Sleep(550 * time.Millisecond)

		// Second alert should be sent automatically after backoff
		assert.GreaterOrEqual(counter, 2)

		webhook.Stop()
	})

	t.Run("alert, then reset, then heartbeat", func(t *testing.T) {
		counter := 0

		onDo := func(b *http.Request) (*http.Response, error) {
			assert.NotNil(b)
			var actualBody WebhookEvent
			if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
				t.Fatalf("not expecting error: %s", err)
			}

			if strings.Contains(actualBody.Schema, "heartbeat") {
				assert.Equal(expectedHeartbeatRequest.Body, actualBody)
				assert.Equal(expectedHeartbeatRequest.Method, b.Method)
				assert.Equal(expectedHeartbeatRequest.URL, b.URL.String())

				counter++
			} else if strings.Contains(actualBody.Schema, "alert") {
				assert.Equal(expectedAlertRequest.Body, actualBody)
				assert.Equal(expectedAlertRequest.Method, b.Method)
				assert.Equal(expectedAlertRequest.URL, b.URL.String())

				counter++
			}

			return nil, nil
		}

		sr := TestWebhookSender{onDo: onDo}
		alertChan := make(chan error, 1)

		webhook := NewWebhookMonitoring("snowbridge", "3.4.0", &sr, "https://test.webhook.com", nil, time.Second, alertChan)
		assert.NotNil(webhook)

		webhook.Start()

		// Sent an error in
		alertChan <- fmt.Errorf("failed to connect to target API")

		// And expect counter to increase by 1 (along side with expected alert event)
		time.Sleep(50 * time.Millisecond) // barely needed to allow enough time for webhook to process event
		assert.Equal(counter, 1)

		// Then, setup error gets resolved
		alertChan <- nil

		// Here we confirm that once setup error gets resolved, we can continue with sending heartbeats as before
		time.Sleep(1100 * time.Millisecond)
		assert.Equal(counter, 2)

		webhook.Stop()
	})

	t.Run("continuous alerts stopped when error resolved", func(t *testing.T) {
		alertCount := 0
		heartbeatCount := 0

		onDo := func(b *http.Request) (*http.Response, error) {
			assert.NotNil(b)

			var actualBody WebhookEvent
			if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
				t.Fatalf("not expecting error: %s", err)
			}

			if strings.Contains(actualBody.Schema, "alert") {
				assert.Equal(expectedAlertRequest.Body, actualBody)
				alertCount++
			} else if strings.Contains(actualBody.Schema, "heartbeat") {
				assert.Equal(expectedHeartbeatRequest.Body, actualBody)
				heartbeatCount++
			}

			return nil, nil
		}

		sr := TestWebhookSender{onDo: onDo}
		alertChan := make(chan error, 1)

		webhook := NewWebhookMonitoring("snowbridge", "3.4.0", &sr, "https://test.webhook.com", nil, 300*time.Millisecond, alertChan)
		assert.NotNil(webhook)

		webhook.Start()

		// Send initial error
		alertChan <- fmt.Errorf("failed to connect to target API")

		// Expect first alert immediately
		time.Sleep(50 * time.Millisecond)
		assert.Equal(alertCount, 1)
		assert.Equal(heartbeatCount, 0)

		// Wait for first backoff period - should send second alert
		time.Sleep(350 * time.Millisecond)
		assert.Equal(alertCount, 2)
		assert.Equal(heartbeatCount, 0)

		// Resolve error before next backoff
		alertChan <- nil

		// Wait past backoff period - should resume heartbeats instead of alerts
		time.Sleep(350 * time.Millisecond)
		assert.Equal(alertCount, 2)     // No more alerts
		assert.Equal(heartbeatCount, 1) // Heartbeat resumed

		webhook.Stop()
	})
}
