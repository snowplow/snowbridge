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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- Test MonitoringSender

type TestMonitoringSender struct {
	onDo func(b *http.Request) (*http.Response, error)
}

func (s *TestMonitoringSender) Do(b *http.Request) (*http.Response, error) {
	return s.onDo(b)
}

// --- Tests

func TestMonitoringHeartbeatTargetWrite(t *testing.T) {
	assert := assert.New(t)

	expectedRequest := struct {
		Method string
		URL    string
		Body   MonitoringEvent
	}{
		Method: "POST",
		URL:    "https://test.webhook.com",
		Body: MonitoringEvent{
			Schema: "iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0",
			Data: MonitoringData{
				AppName:    "snowbridge",
				AppVersion: "3.2.3",
			},
		},
	}

	counter := 0

	onDo := func(b *http.Request) (*http.Response, error) {
		assert.NotNil(b)
		assert.Equal(expectedRequest.Method, b.Method)
		assert.Equal(expectedRequest.URL, b.URL.String())

		var actualBody MonitoringEvent
		if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
			t.Fatalf("not expecting error: %s", err)
		}

		assert.Equal(expectedRequest.Body, actualBody)

		counter++
		return nil, nil
	}

	sr := TestMonitoringSender{onDo: onDo}

	monitoring := NewMonitoring("snowbridge", "3.2.3", &sr, "https://test.webhook.com", nil, time.Second, nil)
	assert.NotNil(monitoring)
	monitoring.Start()

	time.Sleep(2200 * time.Millisecond)
	assert.Equal(counter, 2)

	monitoring.Stop()
}

func TestMonitoringAlertTargetWrite(t *testing.T) {
	assert := assert.New(t)

	expectedRequest := struct {
		Method string
		URL    string
		Body   MonitoringEvent
	}{
		Method: "POST",
		URL:    "https://test.webhook.com",
		Body: MonitoringEvent{
			Schema: "iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0",
			Data: MonitoringData{
				AppName:    "snowbridge",
				AppVersion: "3.2.3",
				Message:    "failed to connect to target API",
			},
		},
	}

	counter := 0

	onDo := func(b *http.Request) (*http.Response, error) {
		assert.NotNil(b)
		assert.Equal(expectedRequest.Method, b.Method)
		assert.Equal(expectedRequest.URL, b.URL.String())

		var actualBody MonitoringEvent
		if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
			t.Fatalf("not expecting error: %s", err)
		}

		assert.Equal(expectedRequest.Body, actualBody)

		counter++
		return nil, nil
	}

	sr := TestMonitoringSender{onDo: onDo}
	alertChan := make(chan error, 1)

	monitoring := NewMonitoring("snowbridge", "3.2.3", &sr, "https://test.webhook.com", nil, (time.Second * 2), alertChan)
	assert.NotNil(monitoring)

	monitoring.Start()

	// Sent an error in
	alertChan <- fmt.Errorf("failed to connect to target API")

	// And expect counter to increase by 1 (along side with expected alert event)
	time.Sleep(50 * time.Millisecond) // barely needed to allow enough time for monitoring to process event
	assert.Equal(counter, 1)

	// Then, we another alert gets sent in, but it is not being sent
	alertChan <- fmt.Errorf("failed to connect to target API")

	// Here we confirm that nor new alert nor heartbeat were sent
	time.Sleep(2500 * time.Millisecond)
	assert.Equal(counter, 1)

	monitoring.Stop()
}
