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
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeConsole struct {
	server *httptest.Server
	calls  atomic.Int32
}

func newFakeConsole(t *testing.T, status int, body string) *fakeConsole {
	t.Helper()
	c := &fakeConsole{}
	c.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.calls.Add(1)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/api/msc/internal/authz/query/v1/org-1/resource/read", r.URL.Path)
		assert.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(c.server.Close)
	return c
}

func (c *fakeConsole) authorizer() *ConsoleAuthorizer {
	return NewConsoleAuthorizer(c.server.URL+"/", "org-1", "resource", "read")
}

func TestConsoleAuthorizerAllows(t *testing.T) {
	console := newFakeConsole(t, http.StatusOK, "true\n")

	ok, err := console.authorizer().Authorize(context.Background(), "Bearer token")

	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int32(1), console.calls.Load())
}

func TestConsoleAuthorizerDenies(t *testing.T) {
	for name, tc := range map[string]struct {
		status int
		body   string
	}{
		"false body":   {status: http.StatusOK, body: "false"},
		"other body":   {status: http.StatusOK, body: `{"allowed":true}`},
		"empty body":   {status: http.StatusOK, body: ""},
		"unauthorized": {status: http.StatusUnauthorized, body: "true"},
		"forbidden":    {status: http.StatusForbidden, body: "true"},
	} {
		t.Run(name, func(t *testing.T) {
			console := newFakeConsole(t, tc.status, tc.body)

			ok, err := console.authorizer().Authorize(context.Background(), "Bearer token")

			require.NoError(t, err)
			assert.False(t, ok)
		})
	}
}

func TestConsoleAuthorizerError(t *testing.T) {
	for name, status := range map[string]int{
		"internal error": http.StatusInternalServerError,
		"not found":      http.StatusNotFound,
		"bad gateway":    http.StatusBadGateway,
	} {
		t.Run(name, func(t *testing.T) {
			console := newFakeConsole(t, status, "true")

			ok, err := console.authorizer().Authorize(context.Background(), "Bearer token")

			require.Error(t, err)
			assert.False(t, ok)
		})
	}
}

func TestConsoleAuthorizerEmptyHeader(t *testing.T) {
	console := newFakeConsole(t, http.StatusOK, "true")

	ok, err := console.authorizer().Authorize(context.Background(), "")

	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, int32(0), console.calls.Load())
}

func TestConsoleAuthorizerCachesAllowed(t *testing.T) {
	console := newFakeConsole(t, http.StatusOK, "true")
	authz := console.authorizer()
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	authz.now = func() time.Time { return now }

	for range 2 {
		ok, err := authz.Authorize(context.Background(), "Bearer token")
		require.NoError(t, err)
		assert.True(t, ok)
	}
	now = now.Add(59 * time.Second)
	ok, err := authz.Authorize(context.Background(), "Bearer token")

	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int32(1), console.calls.Load())
}

func TestConsoleAuthorizerRechecksAfterTTL(t *testing.T) {
	console := newFakeConsole(t, http.StatusOK, "true")
	authz := console.authorizer()
	now := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	authz.now = func() time.Time { return now }

	ok, err := authz.Authorize(context.Background(), "Bearer token")
	require.NoError(t, err)
	require.True(t, ok)
	now = now.Add(time.Minute)
	ok, err = authz.Authorize(context.Background(), "Bearer token")

	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, int32(2), console.calls.Load())
}

func TestConsoleAuthorizerDoesNotCacheDenied(t *testing.T) {
	console := newFakeConsole(t, http.StatusOK, "false")
	authz := console.authorizer()

	for range 2 {
		ok, err := authz.Authorize(context.Background(), "Bearer token")
		require.NoError(t, err)
		assert.False(t, ok)
	}

	assert.Equal(t, int32(2), console.calls.Load())
}
