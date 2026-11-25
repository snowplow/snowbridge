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

package outcomes

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postCollect(t *testing.T, store Store, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/collect", strings.NewReader(body))
	rec := httptest.NewRecorder()
	NewIngestHandler(store).ServeHTTP(rec, req)
	return rec
}

func sentSamples(t *testing.T, body string) []string {
	t.Helper()
	var event struct {
		Data struct {
			InvalidErrors []struct {
				Sample json.RawMessage `json:"sample"`
			} `json:"invalidErrors"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &event))
	var samples []string
	for _, e := range event.Data.InvalidErrors {
		samples = append(samples, string(e.Sample))
	}
	return samples
}

var (
	testPeriodStart = time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	testPeriodEnd   = time.Date(2026, 11, 16, 10, 5, 0, 0, time.UTC)
)

func TestIngestStoresInvalidErrorsWithSample(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": 10,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test destination rejected the event",
					"count": 3,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:01:00Z",
								"errorType": "api",
								"errorCode": "E1",
								"errorMessage": "test destination rejected the event",
								"latestState": "{\"testField\":\"test-value-1\"}"
							},
							"payload": "test-payload-1"
						}
					}
				},
				{
					"code": "E2",
					"description": "test field is missing",
					"count": 7,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:02:00Z",
								"errorType": "api",
								"errorCode": "E2",
								"errorMessage": "test field is missing",
								"latestState": "{\"testField\":\"test-value-2\"}"
							},
							"payload": "test-payload-2"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	store := &fakeStore{}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, store.inserts, 1)
	rows := store.inserts[0]
	require.Len(t, rows, 2)
	samples := sentSamples(t, body)
	assert.Equal(t, testPeriodStart, rows[0].PeriodStart)
	assert.Equal(t, testPeriodEnd, rows[0].PeriodEnd)
	assert.Equal(t, "E1", rows[0].Code)
	assert.Equal(t, "test destination rejected the event", rows[0].Description)
	assert.Equal(t, 3, rows[0].Count)
	assert.JSONEq(t, samples[0], string(rows[0].Sample))
	assert.Equal(t, testPeriodStart, rows[1].PeriodStart)
	assert.Equal(t, testPeriodEnd, rows[1].PeriodEnd)
	assert.Equal(t, "E2", rows[1].Code)
	assert.Equal(t, "test field is missing", rows[1].Description)
	assert.Equal(t, 7, rows[1].Count)
	assert.JSONEq(t, samples[1], string(rows[1].Sample))
}

func TestIngestSkipsInvalidErrorsWithoutSample(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": 7,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test error without sample",
					"count": 2
				},
				{
					"code": "E2",
					"description": "test field is missing",
					"count": 5,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:02:00Z",
								"errorType": "api",
								"errorCode": "E2",
								"errorMessage": "test field is missing",
								"latestState": "{\"testField\":\"test-value-2\"}"
							},
							"payload": "test-payload-2"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	store := &fakeStore{}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, store.inserts, 1)
	require.Len(t, store.inserts[0], 1)
	assert.Equal(t, "E2", store.inserts[0][0].Code)
}

func TestIngestSkipsInvalidErrorsWithNullSample(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": 7,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test error with null sample",
					"count": 2,
					"sample": null
				},
				{
					"code": "E2",
					"description": "test field is missing",
					"count": 5,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:02:00Z",
								"errorType": "api",
								"errorCode": "E2",
								"errorMessage": "test field is missing",
								"latestState": "{\"testField\":\"test-value-2\"}"
							},
							"payload": "test-payload-2"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	store := &fakeStore{}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, store.inserts, 1)
	require.Len(t, store.inserts[0], 1)
	assert.Equal(t, "E2", store.inserts[0][0].Code)
}

func TestIngestIgnoresFailedErrors(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 9,
			"invalidCount": 1,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test destination rejected the event",
					"count": 1,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:01:00Z",
								"errorType": "api",
								"errorCode": "E1",
								"errorMessage": "test destination rejected the event",
								"latestState": "{\"testField\":\"test-value-1\"}"
							},
							"payload": "test-payload-1"
						}
					}
				}
			],
			"failedErrors": [
				{
					"code": "T1",
					"description": "test destination timed out",
					"count": 9,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:03:00Z",
								"errorType": "api",
								"errorCode": "T1",
								"errorMessage": "test destination timed out",
								"latestState": "{\"testField\":\"test-value-3\"}"
							},
							"payload": "test-payload-3"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	store := &fakeStore{}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.Len(t, store.inserts, 1)
	require.Len(t, store.inserts[0], 1)
	assert.Equal(t, "E1", store.inserts[0][0].Code)
}

func TestIngestNothingToStore(t *testing.T) {
	for name, body := range map[string]string{
		"no errors": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 0,
				"invalidErrors": [],
				"failedErrors": [],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"no error fields": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 0,
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"only without sample": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 1,
				"invalidErrors": [
					{
						"code": "E1",
						"description": "test error without sample",
						"count": 1
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"only null sample": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 1,
				"invalidErrors": [
					{
						"code": "E1",
						"description": "test error with null sample",
						"count": 1,
						"sample": null
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"only failed errors": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 1,
				"invalidCount": 0,
				"failedErrors": [
					{
						"code": "T1",
						"description": "test destination timed out",
						"count": 1,
						"sample": {
							"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
							"data": {
								"processor": {
									"artifact": "snowbridge",
									"version": "0.0.1-test"
								},
								"failure": {
									"timestamp": "2026-11-16T10:03:00Z",
									"errorType": "api",
									"errorCode": "T1",
									"errorMessage": "test destination timed out",
									"latestState": "{\"testField\":\"test-value-3\"}"
								},
								"payload": "test-payload-3"
							}
						}
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}

			rec := postCollect(t, store, body)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Empty(t, store.inserts)
		})
	}
}

func TestIngestStoreError(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": 1,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test destination rejected the event",
					"count": 1,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:01:00Z",
								"errorType": "api",
								"errorCode": "E1",
								"errorMessage": "test destination rejected the event",
								"latestState": "{\"testField\":\"test-value-1\"}"
							},
							"payload": "test-payload-1"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	store := &fakeStore{insertErr: errors.New("test store failure")}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.JSONEq(t, `{"message":"cannot write errors"}`, rec.Body.String())
	require.Len(t, store.inserts, 1)
	require.Len(t, store.inserts[0], 1)
	assert.Equal(t, "E1", store.inserts[0][0].Code)
}

func TestIngestInvalidJSON(t *testing.T) {
	body := `not json`
	store := &fakeStore{}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.JSONEq(t, `{"message":"invalid metadata event"}`, rec.Body.String())
	assert.Empty(t, store.inserts)
}

func TestIngestInvalidPeriod(t *testing.T) {
	for name, body := range map[string]string{
		"missing start": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 1,
				"invalidErrors": [
					{
						"code": "E1",
						"description": "test destination rejected the event",
						"count": 1,
						"sample": {
							"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
							"data": {
								"processor": {
									"artifact": "snowbridge",
									"version": "0.0.1-test"
								},
								"failure": {
									"timestamp": "2026-11-16T10:01:00Z",
									"errorType": "api",
									"errorCode": "E1",
									"errorMessage": "test destination rejected the event",
									"latestState": "{\"testField\":\"test-value-1\"}"
								},
								"payload": "test-payload-1"
							}
						}
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"missing end": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 1,
				"invalidErrors": [
					{
						"code": "E1",
						"description": "test destination rejected the event",
						"count": 1,
						"sample": {
							"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
							"data": {
								"processor": {
									"artifact": "snowbridge",
									"version": "0.0.1-test"
								},
								"failure": {
									"timestamp": "2026-11-16T10:01:00Z",
									"errorType": "api",
									"errorCode": "E1",
									"errorMessage": "test destination rejected the event",
									"latestState": "{\"testField\":\"test-value-1\"}"
								},
								"payload": "test-payload-1"
							}
						}
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"unparseable start": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "yesterday",
				"periodEnd": "2026-11-16T10:05:00Z",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 1,
				"invalidErrors": [
					{
						"code": "E1",
						"description": "test destination rejected the event",
						"count": 1,
						"sample": {
							"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
							"data": {
								"processor": {
									"artifact": "snowbridge",
									"version": "0.0.1-test"
								},
								"failure": {
									"timestamp": "2026-11-16T10:01:00Z",
									"errorType": "api",
									"errorCode": "E1",
									"errorMessage": "test destination rejected the event",
									"latestState": "{\"testField\":\"test-value-1\"}"
								},
								"payload": "test-payload-1"
							}
						}
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
		"unparseable end": `{
			"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
			"data": {
				"appName": "test-app",
				"appVersion": "0.0.1-test",
				"periodStart": "2026-11-16T10:00:00Z",
				"periodEnd": "today",
				"successCount": 100,
				"filteredCount": 2,
				"failedCount": 0,
				"invalidCount": 1,
				"invalidErrors": [
					{
						"code": "E1",
						"description": "test destination rejected the event",
						"count": 1,
						"sample": {
							"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
							"data": {
								"processor": {
									"artifact": "snowbridge",
									"version": "0.0.1-test"
								},
								"failure": {
									"timestamp": "2026-11-16T10:01:00Z",
									"errorType": "api",
									"errorCode": "E1",
									"errorMessage": "test destination rejected the event",
									"latestState": "{\"testField\":\"test-value-1\"}"
								},
								"payload": "test-payload-1"
							}
						}
					}
				],
				"tags": {
					"pipeline": "test-pipeline"
				}
			}
		}`,
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}

			rec := postCollect(t, store, body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.JSONEq(t, `{"message":"invalid period"}`, rec.Body.String())
			assert.Empty(t, store.inserts)
		})
	}
}

func TestIngestBodyTooLarge(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": 1,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test destination rejected the event",
					"count": 1,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:01:00Z",
								"errorType": "api",
								"errorCode": "E1",
								"errorMessage": "test destination rejected the event",
								"latestState": "{\"testField\":\"test-value-1\"}"
							},
							"payload": "PADDING"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	body = strings.Replace(body, "PADDING", strings.Repeat("a", maxIngestBody), 1)
	store := &fakeStore{}

	rec := postCollect(t, store, body)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Empty(t, store.inserts)
}

func TestIngestRejectsOtherRoutes(t *testing.T) {
	body := `{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": "2026-11-16T10:00:00Z",
			"periodEnd": "2026-11-16T10:05:00Z",
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": 1,
			"invalidErrors": [
				{
					"code": "E1",
					"description": "test destination rejected the event",
					"count": 1,
					"sample": {
						"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
						"data": {
							"processor": {
								"artifact": "snowbridge",
								"version": "0.0.1-test"
							},
							"failure": {
								"timestamp": "2026-11-16T10:01:00Z",
								"errorType": "api",
								"errorCode": "E1",
								"errorMessage": "test destination rejected the event",
								"latestState": "{\"testField\":\"test-value-1\"}"
							},
							"payload": "test-payload-1"
						}
					}
				}
			],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`
	for name, tc := range map[string]struct {
		method string
		path   string
	}{
		"GET /collect":    {method: http.MethodGet, path: "/collect"},
		"PUT /collect":    {method: http.MethodPut, path: "/collect"},
		"POST other path": {method: http.MethodPost, path: "/other"},
		"POST root":       {method: http.MethodPost, path: "/"},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{}
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(body))
			rec := httptest.NewRecorder()

			NewIngestHandler(store).ServeHTTP(rec, req)

			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)
			assert.Empty(t, store.inserts)
		})
	}
}
