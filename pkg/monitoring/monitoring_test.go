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

func TestObserverTargetWrite(t *testing.T) {
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
		},
	}

	counter := 0

	onDo := func(b *http.Request) (*http.Response, error) {
		assert.NotNil(b)
		assert.Equal(expectedRequest.Method, b.Method)
		assert.Equal(expectedRequest.URL, b.URL.String())

		var actualBody MonitoringEvent
		json.NewDecoder(b.Body).Decode(&actualBody)

		assert.Equal(expectedRequest.Body.Schema, actualBody.Schema)

		counter++
		return nil, nil
	}

	sr := TestMonitoringSender{onDo: onDo}

	observer := NewMonitoring("snowbridge", "3.2.3", &sr, "https://test.webhook.com", nil, time.Second, nil)
	assert.NotNil(observer)
	observer.Start()

	time.Sleep(2200 * time.Millisecond)
	assert.Equal(counter, 2)

	observer.Stop()
}
