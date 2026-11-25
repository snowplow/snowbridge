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
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/v6/pkg/models"
	"github.com/stretchr/testify/assert"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
				AppVersion:  "3.4.0",
				PeriodStart: now.Format(time.RFC3339),
				PeriodEnd:   now.Format(time.RFC3339),
				Success:     7,
				Failed:      3,
				FailedErrors: []AggregatedError{
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

			if diff := cmp.Diff(
				expectedMetadataRequest.Body,
				actualBody,
				cmpopts.SortSlices(
					func(a, b AggregatedError) bool {
						return a.Code < b.Code
					}),
			); diff != "" {
				t.Fatalf("unexpected body (-want +got):\n%s", diff)
			}

			assert.Equal(expectedMetadataRequest.Method, b.Method)
			assert.Equal(expectedMetadataRequest.URL, b.URL.String())

			counter++
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}

		mr := &TestMetadataReporter{onDo: onDo}

		webhook := NewMetadataReporter("snowbridge", "3.4.0", mr, "https://test.metadatareporter.com", nil, false)
		assert.NotNil(webhook)
		buffer := &models.ObserverBuffer{
			TargetResults: 10,
			MsgSent:       7,
			MsgFailed:     3,
			InvalidErrors: map[models.MetadataCodeDescription]*models.ErrorAggregate{},
			FailedErrors: map[models.MetadataCodeDescription]int{
				{
					Code:        "400 Bad Request",
					Description: "bad request",
				}: 1,
				{
					Code:        "",
					Description: "some error",
				}: 1,
				{
					Code:        "SyntaxError",
					Description: "SyntaxError",
				}: 1,
			},
		}

		webhook.Send(buffer, now, now)
	})
}

func TestMetadataReporterRetries(t *testing.T) {
	now := time.Now()
	buffer := &models.ObserverBuffer{}

	t.Run("retries on transport error then succeeds", func(t *testing.T) {
		calls := 0
		onDo := func(*http.Request) (*http.Response, error) {
			calls++
			if calls < 3 {
				return nil, errors.New("connection refused")
			}
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}
		mr := NewMetadataReporter("snowbridge", "3.4.0", &TestMetadataReporter{onDo: onDo}, "https://test.metadatareporter.com", nil, false)
		mr.Send(buffer, now, now)
		assert.Equal(t, 3, calls)
	})

	t.Run("retries on 5xx then succeeds", func(t *testing.T) {
		calls := 0
		onDo := func(*http.Request) (*http.Response, error) {
			calls++
			if calls < 3 {
				return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
			}
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}
		mr := NewMetadataReporter("snowbridge", "3.4.0", &TestMetadataReporter{onDo: onDo}, "https://test.metadatareporter.com", nil, false)
		mr.Send(buffer, now, now)
		assert.Equal(t, 3, calls)
	})

	t.Run("stops after 6 attempts on persistent transport error", func(t *testing.T) {
		calls := 0
		onDo := func(*http.Request) (*http.Response, error) {
			calls++
			return nil, errors.New("connection refused")
		}
		mr := NewMetadataReporter("snowbridge", "3.4.0", &TestMetadataReporter{onDo: onDo}, "https://test.metadatareporter.com", nil, false)
		mr.Send(buffer, now, now)
		assert.Equal(t, 6, calls)
	})

	t.Run("stops after 6 attempts on persistent 5xx", func(t *testing.T) {
		calls := 0
		onDo := func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
		}
		mr := NewMetadataReporter("snowbridge", "3.4.0", &TestMetadataReporter{onDo: onDo}, "https://test.metadatareporter.com", nil, false)
		mr.Send(buffer, now, now)
		assert.Equal(t, 6, calls)
	})
}

// TestMetadataReporterErrorSamples checks that samples reach the payload only when the
// reporter is configured to include them, and never for failed errors.
func TestMetadataReporterErrorSamples(t *testing.T) {
	now := time.Now()

	invalidKey := models.MetadataCodeDescription{Code: "TransformationError", Description: "could not parse"}
	failedKey := models.MetadataCodeDescription{Code: "500 Internal Server Error", Description: "server error"}

	invalidBadRow := `{"schema":"iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1","data":{"payload":"web\toriginal-tsv"}}`

	newBuffer := func() *models.ObserverBuffer {
		return &models.ObserverBuffer{
			InvalidMsgSent: 2,
			MsgFailed:      1,
			InvalidErrors: map[models.MetadataCodeDescription]*models.ErrorAggregate{
				invalidKey: {Count: 2, Sample: []byte(invalidBadRow)},
			},
			FailedErrors: map[models.MetadataCodeDescription]int{
				failedKey: 1,
			},
		}
	}

	// sendAndCapture runs one Send and returns the decoded event alongside the raw body,
	// so we can also assert on the omitempty behaviour of the sample field.
	sendAndCapture := func(t *testing.T, buffer *models.ObserverBuffer, includeSamples bool) (MetadataEvent, string) {
		t.Helper()

		var rawBody []byte
		onDo := func(r *http.Request) (*http.Response, error) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("not expecting error: %s", err)
			}
			rawBody = body
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}

		mr := NewMetadataReporter("snowbridge", "3.4.0", &TestMetadataReporter{onDo: onDo}, "https://test.metadatareporter.com", nil, includeSamples)
		mr.Send(buffer, now, now)

		var event MetadataEvent
		if err := json.Unmarshal(rawBody, &event); err != nil {
			t.Fatalf("not expecting error: %s", err)
		}
		return event, string(rawBody)
	}

	t.Run("includes the sample when enabled", func(t *testing.T) {
		event, _ := sendAndCapture(t, newBuffer(), true)

		assert.Equal(t, 1, len(event.Data.InvalidErrors))
		assert.Equal(t, 2, event.Data.InvalidErrors[0].Count)
		assert.JSONEq(t, invalidBadRow, string(event.Data.InvalidErrors[0].Sample))
	})

	t.Run("omits the sample when disabled", func(t *testing.T) {
		event, rawBody := sendAndCapture(t, newBuffer(), false)

		assert.Equal(t, 1, len(event.Data.InvalidErrors))
		assert.Equal(t, 2, event.Data.InvalidErrors[0].Count)
		assert.Empty(t, event.Data.InvalidErrors[0].Sample)
		assert.NotContains(t, rawBody, `"sample"`)
	})

	t.Run("never includes a sample for failed errors", func(t *testing.T) {
		event, rawBody := sendAndCapture(t, newBuffer(), true)

		assert.Equal(t, 1, len(event.Data.FailedErrors))
		assert.Equal(t, 1, event.Data.FailedErrors[0].Count)
		assert.Empty(t, event.Data.FailedErrors[0].Sample)
		assert.NotContains(t, rawBody, "should-never-be-sent")
	})
}

func TestMaxErrorSamples(t *testing.T) {
	withSamples := NewMetadataReporter("app", "1.0.0", nil, "http://a", nil, true)
	withoutSamples := NewMetadataReporter("app", "1.0.0", nil, "http://b", nil, false)

	assert.Equal(t, 0, MaxErrorSamples(nil))
	assert.Equal(t, 0, MaxErrorSamples(withoutSamples))
	assert.Equal(t, errorSamplesPerPeriod, MaxErrorSamples(withSamples))
	assert.Equal(t, 0, MaxErrorSamples(FanOut([]MetadataReporterer{withoutSamples, withoutSamples})))
	assert.Equal(t, errorSamplesPerPeriod, MaxErrorSamples(FanOut([]MetadataReporterer{withoutSamples, withSamples})))
}
