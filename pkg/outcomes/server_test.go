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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	keys      []ErrorKey
	sample    *ErrorSample
	err       error
	pingErr   error
	insertErr error
	inserts   [][]ErrorRow
}

func (f *fakeStore) ErrorsWithSample(context.Context, time.Time, int) ([]ErrorKey, error) {
	return f.keys, f.err
}

func (f *fakeStore) LatestSample(context.Context, string, string) (*ErrorSample, error) {
	return f.sample, f.err
}

func (f *fakeStore) Insert(_ context.Context, rows []ErrorRow) error {
	f.inserts = append(f.inserts, rows)
	return f.insertErr
}

func (f *fakeStore) Ping(context.Context) error {
	return f.pingErr
}

type fakeAuthorizer struct{}

func (fakeAuthorizer) Authorize(_ context.Context, authHeader string) (bool, error) {
	return authHeader == "Bearer valid", nil
}

func TestErrorList(t *testing.T) {
	store := &fakeStore{keys: []ErrorKey{
		{Code: "400", Description: "Bad Request"},
		{Code: "timeout", Description: "request timed out"},
	}}
	req := httptest.NewRequest(http.MethodGet, "/event-forwarding/v1/errors", nil)
	req.Header.Set("Authorization", "Bearer valid")
	rec := httptest.NewRecorder()

	NewHandler(store, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Equal(t, "https://console.example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.JSONEq(t, `{"errors":[{"code":"400","description":"Bad Request"},{"code":"timeout","description":"request timed out"}]}`, rec.Body.String())
}

func TestErrorListEmpty(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/event-forwarding/v1/errors", nil)
	req.Header.Set("Authorization", "Bearer valid")
	rec := httptest.NewRecorder()

	NewHandler(&fakeStore{}, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"errors":[]}`, rec.Body.String())
}

func TestErrorListFrom(t *testing.T) {
	for name, query := range map[string]string{
		"missing":  "",
		"explicit": "?from=2026-11-01T00:00:00Z",
		"empty":    "?from=",
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/event-forwarding/v1/errors"+query, nil)
			req.Header.Set("Authorization", "Bearer valid")
			rec := httptest.NewRecorder()

			NewHandler(&fakeStore{}, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

func TestErrorListBadFrom(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/event-forwarding/v1/errors?from=yesterday", nil)
	req.Header.Set("Authorization", "Bearer valid")
	rec := httptest.NewRecorder()

	NewHandler(&fakeStore{}, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var body apiError
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotEmpty(t, body.Message)
}

func TestErrorListForbidden(t *testing.T) {
	for name, auth := range map[string]string{"missing": "", "invalid": "Bearer nope"} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/event-forwarding/v1/errors", nil)
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}
			rec := httptest.NewRecorder()

			NewHandler(&fakeStore{}, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.JSONEq(t, `{"message":"Authentication denied"}`, rec.Body.String())
		})
	}
}

const sampleURL = "/event-forwarding/v1/errors/sample?code=400&description=Bad+Request"

func ptr(s string) *string {
	return &s
}

func enrichedTSV() string {
	fields := make([]string, 131)
	fields[0] = "test-app"
	fields[1] = "web"
	fields[3] = "2026-11-16 10:00:00.000"
	fields[5] = "page_view"
	fields[6] = "e9234345-f042-46ad-b1aa-424464066a33"
	return strings.Join(fields, "\t")
}

func getSample(t *testing.T, store Store, url, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	NewHandler(store, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)
	return rec
}

func TestErrorSample(t *testing.T) {
	store := &fakeStore{sample: &ErrorSample{
		SeenAt:       time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC),
		Input:        ptr(`{"a":1}`),
		LatestState:  ptr("not json"),
		ErrorMessage: ptr("boom"),
		ErrorType:    ptr("api"),
	}}

	rec := getSample(t, store, sampleURL, "Bearer valid")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"seenAt":"2026-11-16T10:00:00Z","input":{"a":1},"latestState":"not json","errorMessage":"boom","errorType":"api"}`, rec.Body.String())
}

func TestErrorSampleWithoutErrorType(t *testing.T) {
	store := &fakeStore{sample: &ErrorSample{
		SeenAt:       time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC),
		Input:        ptr(`{"a":1}`),
		LatestState:  ptr("not json"),
		ErrorMessage: ptr("boom"),
	}}

	rec := getSample(t, store, sampleURL, "Bearer valid")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"seenAt":"2026-11-16T10:00:00Z","input":{"a":1},"latestState":"not json","errorMessage":"boom","errorType":null}`, rec.Body.String())
}

func TestErrorSampleNotFound(t *testing.T) {
	rec := getSample(t, &fakeStore{}, sampleURL, "Bearer valid")

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.JSONEq(t, `{"message":"no sample found for error code '400'"}`, rec.Body.String())
}

func TestErrorSampleMissingParams(t *testing.T) {
	for name, query := range map[string]string{
		"no code":        "?description=Bad+Request",
		"no description": "?code=400",
		"neither":        "",
	} {
		t.Run(name, func(t *testing.T) {
			rec := getSample(t, &fakeStore{sample: &ErrorSample{}}, "/event-forwarding/v1/errors/sample"+query, "Bearer valid")

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.JSONEq(t, `{"message":"code and description are required"}`, rec.Body.String())
		})
	}
}

func TestErrorSampleForbidden(t *testing.T) {
	for name, auth := range map[string]string{"missing": "", "invalid": "Bearer nope"} {
		t.Run(name, func(t *testing.T) {
			rec := getSample(t, &fakeStore{sample: &ErrorSample{}}, sampleURL, auth)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.JSONEq(t, `{"message":"Authentication denied"}`, rec.Body.String())
		})
	}
}

func TestErrorSampleStoreError(t *testing.T) {
	rec := getSample(t, &fakeStore{err: errors.New("db down")}, sampleURL, "Bearer valid")

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.JSONEq(t, `{"message":"cannot read error sample"}`, rec.Body.String())
}

func TestErrorSamplePayloadForms(t *testing.T) {
	for name, tc := range map[string]struct {
		payload *string
		check   func(t *testing.T, got json.RawMessage)
	}{
		"enriched TSV": {
			payload: ptr(enrichedTSV()),
			check: func(t *testing.T, got json.RawMessage) {
				var event map[string]any
				require.NoError(t, json.Unmarshal(got, &event))
				assert.Equal(t, "test-app", event["app_id"])
				assert.Equal(t, "page_view", event["event"])
				assert.Equal(t, "e9234345-f042-46ad-b1aa-424464066a33", event["event_id"])
			},
		},
		"valid JSON": {
			payload: ptr(`{"nested":{"list":[1,2]}}`),
			check: func(t *testing.T, got json.RawMessage) {
				assert.JSONEq(t, `{"nested":{"list":[1,2]}}`, string(got))
			},
		},
		"other string": {
			payload: ptr("plain\ttext"),
			check: func(t *testing.T, got json.RawMessage) {
				assert.JSONEq(t, `"plain\ttext"`, string(got))
			},
		},
		"missing": {
			payload: nil,
			check: func(t *testing.T, got json.RawMessage) {
				assert.Equal(t, "null", string(got))
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := &fakeStore{sample: &ErrorSample{Input: tc.payload, LatestState: tc.payload}}

			rec := getSample(t, store, sampleURL, "Bearer valid")

			require.Equal(t, http.StatusOK, rec.Code)
			var body map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			tc.check(t, body["input"])
			tc.check(t, body["latestState"])
		})
	}
}

func TestHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	NewHandler(&fakeStore{}, fakeAuthorizer{}, "*").ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "healthy", rec.Body.String())
}

func TestHealthzUnavailable(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	NewHandler(&fakeStore{pingErr: errors.New("db down")}, fakeAuthorizer{}, "*").ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.JSONEq(t, `{"message":"database unreachable"}`, rec.Body.String())
}

func TestCORSPreflight(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/event-forwarding/v1/errors/sample", nil)
	req.Header.Set("Origin", "https://console.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	rec := httptest.NewRecorder()

	NewHandler(&fakeStore{}, fakeAuthorizer{}, "https://console.example.com").ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "https://console.example.com", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "GET, OPTIONS", rec.Header().Get("Access-Control-Allow-Methods"))
	assert.Equal(t, "Authorization, Content-Type", rec.Header().Get("Access-Control-Allow-Headers"))
	assert.Equal(t, "Origin", rec.Header().Get("Vary"))
}

func TestCORSResponse(t *testing.T) {
	for name, tc := range map[string]struct {
		origin string
		vary   string
	}{
		"specific origin": {origin: "https://console.example.com", vary: "Origin"},
		"any origin":      {origin: "*", vary: ""},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			rec := httptest.NewRecorder()

			NewHandler(&fakeStore{}, fakeAuthorizer{}, tc.origin).ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tc.origin, rec.Header().Get("Access-Control-Allow-Origin"))
			assert.Equal(t, tc.vary, rec.Header().Get("Vary"))
		})
	}
}

func topLevelKeys(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	_, err := dec.Token()
	require.NoError(t, err)
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		require.NoError(t, err)
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		require.NoError(t, dec.Decode(&skip))
	}
	return keys
}

func TestErrorSampleEnrichedEventKeysSorted(t *testing.T) {
	fields := make([]string, 131)
	fields[0] = "test-app"
	fields[1] = "web"
	fields[3] = "2026-11-16 10:00:00.000"
	fields[5] = "page_view"
	fields[6] = "e9234345-f042-46ad-b1aa-424464066a33"
	fields[12] = "test-user"
	fields[124] = "2026-11-16 10:00:00.000"
	fields[126] = "page_view"
	store := &fakeStore{sample: &ErrorSample{Input: ptr(strings.Join(fields, "\t"))}}

	rec := getSample(t, store, sampleURL, "Bearer valid")

	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	keys := topLevelKeys(t, body["input"])
	assert.Len(t, keys, 8)
	assert.True(t, slices.IsSorted(keys), "keys not sorted: %v", keys)
}
