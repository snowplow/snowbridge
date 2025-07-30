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

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/stretchr/testify/assert"
)

// --- Test MetadataReporter

type TestMetadataReporter struct {
	onDo func(b *http.Request) (*http.Response, error)
}

func (s *TestMetadataReporter) Do(b *http.Request) (*http.Response, error) {
	return s.onDo(b)
}

// --- Tests

func TestMetadataReporterTargetWrite(t *testing.T) {
	assert := assert.New(t)
	now := time.Now()

	expectedMetadataRequest := struct {
		Method string
		URL    string
		Body   MetadataEvent
	}{
		Method: "POST",
		URL:    "https://test.metadatareporter.com",
		Body: MetadataEvent{
			Schema: "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			Data: MetadataWrapper{
				AppName:     "snowbridge",
				AppVersion:  "3.2.3",
				PeriodStart: now.Format(time.RFC3339),
				PeriodEnd:   now.Format(time.RFC3339),
				Success:     7,
				Failed:      3,
				FailedErrors: []models.AggregatedError{
					{
						Code:        "400 Bad Request",
						Description: "bad request",
						Count:       1,
					},
					{
						Code:        "",
						Description: "some error",
						Count:       1,
					},
					{
						Code:        "SyntaxError",
						Description: "SyntaxError",
						Count:       1,
					},
				},
			},
		},
	}

	t.Run("happy path", func(t *testing.T) {
		counter := 0

		onDo := func(b *http.Request) (*http.Response, error) {
			assert.NotNil(b)

			var actualBody MetadataEvent
			if err := json.NewDecoder(b.Body).Decode(&actualBody); err != nil {
				t.Fatalf("not expecting error: %s", err)
			}

			assert.Equal(expectedMetadataRequest.Body, actualBody)
			assert.Equal(expectedMetadataRequest.Method, b.Method)
			assert.Equal(expectedMetadataRequest.URL, b.URL.String())

			counter++
			return nil, nil
		}

		mr := &TestMetadataReporter{onDo: onDo}

		webhook := NewMetadataReporter("snowbridge", "3.2.3", mr, "https://test.metadatareporter.com", nil)
		assert.NotNil(webhook)
		buffer := &models.ObserverBuffer{
			TargetResults: 10,
			MsgSent:       7,
			MsgFailed:     3,
			MsgTotal:      10,
			InvalidErrors: map[models.SanitisedErrorMetadata]int{},
			FailedErrors: map[models.SanitisedErrorMetadata]int{
				&models.ApiError{
					StatusCode:  "400 Bad Request",
					SafeMessage: "bad request",
				}: 1,
				&models.TemplatingError{
					SafeMessage: "some error",
				}: 1,
				&models.TransformationError{
					SafeMessage: "SyntaxError",
				}: 1,
			},
		}

		webhook.Send(buffer, now, now)
	})
}
