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
	}{
		Method: "POST",
		URL:    "https://test.webhook.com",
	}

	counter := 0

	onDo := func(b *http.Request) (*http.Response, error) {
		assert.NotNil(b)
		assert.Equal(expectedRequest.Method, b.Method)
		assert.Equal(expectedRequest.URL, b.URL.String())

		counter++
		return nil, nil
	}

	sr := TestMonitoringSender{onDo: onDo}

	observer := NewMonitoring(&sr, "test.webhook.com", nil, time.Second, nil)
	assert.NotNil(observer)
	observer.Start()

	time.Sleep(2200 * time.Millisecond)
	assert.Equal(counter, 2)

	observer.Stop()
}
