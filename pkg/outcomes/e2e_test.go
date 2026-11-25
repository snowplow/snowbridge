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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

const e2eToken = "Bearer test-token-0001"

var e2ePostgresDSN string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}

	ctx := context.Background()
	container, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("test-outcomes"),
		postgres.WithUsername("test-user"),
		postgres.WithPassword("test-password"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Fatalf("cannot start postgres: %v", err)
	}
	e2ePostgresDSN, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(container)
		log.Fatalf("cannot get postgres connection string: %v", err)
	}

	code := m.Run()

	if err := testcontainers.TerminateContainer(container); err != nil {
		log.Printf("cannot terminate postgres: %v", err)
	}
	os.Exit(code)
}

type e2eServer struct {
	t       *testing.T
	readURL string
	ingest  string
	stop    func()
}

func newE2EServer(t *testing.T) *e2eServer {
	t.Helper()
	if testing.Short() {
		t.Skip("end-to-end test needs Docker")
	}

	console := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != e2eToken {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte("true"))
	}))
	t.Cleanup(console.Close)
	trustFakeConsoleCertificate(t, console)

	readAddr, ingestAddr := freeAddr(t), freeAddr(t)
	t.Setenv("OUTCOMES_LISTEN_ADDR", readAddr)
	t.Setenv("OUTCOMES_INGEST_ADDR", ingestAddr)
	t.Setenv("OUTCOMES_PG_DSN", e2ePostgresDSN)
	t.Setenv("OUTCOMES_CONSOLE_DOMAIN", console.Listener.Addr().String())
	t.Setenv("OUTCOMES_ORG_ID", "00000000-0000-0000-0000-000000000001")
	t.Setenv("OUTCOMES_AUTHZ_RESOURCE", "test-resource")
	t.Setenv("OUTCOMES_AUTHZ_ACTION", "test-action")
	t.Setenv("OUTCOMES_CORS_ORIGIN", "*")
	t.Setenv("OUTCOMES_TTL_DAYS", "30")

	t.Cleanup(func() { truncateErrors(t) })

	s := &e2eServer{t: t, readURL: "http://" + readAddr, ingest: "http://" + ingestAddr}
	s.start()
	t.Cleanup(func() { s.stop() })
	return s
}

func trustFakeConsoleCertificate(t *testing.T, console *httptest.Server) {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport)
	previous := transport.TLSClientConfig
	roots := x509.NewCertPool()
	roots.AddCert(console.Certificate())
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	t.Cleanup(func() {
		transport.TLSClientConfig = previous
		transport.CloseIdleConnections()
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func truncateErrors(t *testing.T) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), e2ePostgresDSN)
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()
	_, err = conn.Exec(context.Background(), "truncate ef_errors")
	require.NoError(t, err)
}

func (s *e2eServer) start() {
	s.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx) }()

	var once sync.Once
	s.stop = func() {
		once.Do(func() {
			cancel()
			assert.NoError(s.t, <-done)
		})
	}

	var runErr error
	stopped := false
	require.Eventually(s.t, func() bool {
		select {
		case runErr = <-done:
			done <- runErr
			stopped = true
			return true
		default:
		}
		status, _ := s.get("/healthz", "")
		return status == http.StatusOK
	}, 30*time.Second, 100*time.Millisecond)
	require.False(s.t, stopped, "outcomes server stopped before it was ready: %v", runErr)
}

func (s *e2eServer) get(path, authorization string) (int, string) {
	req, err := http.NewRequest(http.MethodGet, s.readURL+path, nil)
	if err != nil {
		return 0, err.Error()
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err.Error()
	}
	return resp.StatusCode, string(body)
}

func (s *e2eServer) collect(body string) {
	s.t.Helper()
	resp, err := http.Post(s.ingest+"/collect", "application/json", strings.NewReader(body))
	require.NoError(s.t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(s.t, http.StatusOK, resp.StatusCode)
}

func (s *e2eServer) sample(code, description string) (int, string) {
	return s.get("/event-forwarding/v1/errors/sample?"+url.Values{"code": {code}, "description": {description}}.Encode(), e2eToken)
}

func (s *e2eServer) errorList(from time.Time) string {
	s.t.Helper()
	path := "/event-forwarding/v1/errors"
	if !from.IsZero() {
		path += "?" + url.Values{"from": {from.Format(time.RFC3339)}}.Encode()
	}
	status, body := s.get(path, e2eToken)
	require.Equal(s.t, http.StatusOK, status)
	return body
}

type e2eError struct {
	code         string
	description  string
	errorMessage string
	payload      string
	latestState  string
	count        int
}

func e2eMetadataEvent(t *testing.T, periodEnd time.Time, errs ...e2eError) string {
	t.Helper()
	periodStart := periodEnd.Add(-5 * time.Minute)
	invalidErrors := make([]string, 0, len(errs))
	invalidCount := 0
	for _, e := range errs {
		count := e.count
		if count == 0 {
			count = 3
		}
		invalidCount += count
		errorMessage := e.errorMessage
		if errorMessage == "" {
			errorMessage = e.description
		}
		invalidErrors = append(invalidErrors, fmt.Sprintf(`{
			"code": %s,
			"description": %s,
			"count": %d,
			"sample": {
				"schema": "iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-1",
				"data": {
					"processor": {
						"artifact": "snowbridge",
						"version": "0.0.1-test"
					},
					"failure": {
						"timestamp": %s,
						"errorType": "api",
						"errorCode": %s,
						"errorMessage": %s,
						"latestState": %s
					},
					"payload": %s
				}
			}
		}`, jsonString(t, e.code), jsonString(t, e.description), count, jsonString(t, periodStart.Add(time.Minute).Format(time.RFC3339)),
			jsonString(t, e.code), jsonString(t, errorMessage), jsonString(t, e.latestState), jsonString(t, e.payload)))
	}
	return fmt.Sprintf(`{
		"schema": "iglu:com.snowplowanalytics.snowplow/event_forwarding_metrics/jsonschema/1-0-0",
		"data": {
			"appName": "test-app",
			"appVersion": "0.0.1-test",
			"periodStart": %s,
			"periodEnd": %s,
			"successCount": 100,
			"filteredCount": 2,
			"failedCount": 0,
			"invalidCount": %d,
			"invalidErrors": [%s],
			"failedErrors": [],
			"tags": {
				"pipeline": "test-pipeline"
			}
		}
	}`, jsonString(t, periodStart.Format(time.RFC3339)), jsonString(t, periodEnd.Format(time.RFC3339)), invalidCount, strings.Join(invalidErrors, ","))
}

func jsonString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}

func TestE2EIngestThenRead(t *testing.T) {
	s := newE2EServer(t)
	periodEnd := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	s.collect(e2eMetadataEvent(t, periodEnd, e2eError{
		code:         "E1",
		description:  "test destination rejected the event",
		errorMessage: "test field is missing",
		payload:      "test-payload-1",
		latestState:  `{"testField":"test-value-1"}`,
	}))

	assert.JSONEq(t, `{"errors":[{"code":"E1","description":"test destination rejected the event"}]}`, s.errorList(time.Time{}))
	status, body := s.sample("E1", "test destination rejected the event")
	assert.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, fmt.Sprintf(`{
		"seenAt": %s,
		"input": "test-payload-1",
		"latestState": {"testField": "test-value-1"},
		"errorMessage": "test field is missing",
		"errorType": "api"
	}`, jsonString(t, periodEnd.Format(time.RFC3339))), body)
}

func TestE2ESameErrorInTwoPeriodsReturnsLaterSample(t *testing.T) {
	s := newE2EServer(t)
	later := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	earlier := later.Add(-time.Hour)
	s.collect(e2eMetadataEvent(t, later, e2eError{
		code:        "E1",
		description: "test destination rejected the event",
		payload:     "test-payload-later",
		latestState: `{"testField":"test-value-later"}`,
	}))
	s.collect(e2eMetadataEvent(t, earlier, e2eError{
		code:        "E1",
		description: "test destination rejected the event",
		payload:     "test-payload-earlier",
		latestState: `{"testField":"test-value-earlier"}`,
	}))

	status, body := s.sample("E1", "test destination rejected the event")
	assert.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, fmt.Sprintf(`{
		"seenAt": %s,
		"input": "test-payload-later",
		"latestState": {"testField": "test-value-later"},
		"errorMessage": "test destination rejected the event",
		"errorType": "api"
	}`, jsonString(t, later.Format(time.RFC3339))), body)
}

func TestE2EErrorListFromWindow(t *testing.T) {
	s := newE2EServer(t)
	now := time.Now().UTC().Truncate(time.Second)
	from := now.Add(-10 * 24 * time.Hour)
	s.collect(e2eMetadataEvent(t, from.Add(5*time.Minute-time.Second), e2eError{
		code:        "E3",
		description: "test error before window",
		payload:     "test-payload-before",
		latestState: `{"testField":"test-value-before"}`,
		count:       20,
	}))
	s.collect(e2eMetadataEvent(t, now.Add(-8*24*time.Hour), e2eError{
		code:        "E1",
		description: "test old error",
		payload:     "test-payload-old",
		latestState: `{"testField":"test-value-old"}`,
		count:       9,
	}))
	s.collect(e2eMetadataEvent(t, now.Add(-24*time.Hour), e2eError{
		code:        "E2",
		description: "test recent error",
		payload:     "test-payload-recent",
		latestState: `{"testField":"test-value-recent"}`,
	}))

	assert.JSONEq(t, `{"errors":[{"code":"E2","description":"test recent error"}]}`, s.errorList(time.Time{}))
	assert.JSONEq(t, `{"errors":[
		{"code":"E1","description":"test old error"},
		{"code":"E2","description":"test recent error"}
	]}`, s.errorList(from))
}

func TestE2ETTLAcrossRestart(t *testing.T) {
	s := newE2EServer(t)
	now := time.Now().UTC().Truncate(time.Second)
	s.collect(e2eMetadataEvent(t, now.Add(-40*24*time.Hour), e2eError{
		code:        "E1",
		description: "test expired error",
		payload:     "test-payload-expired",
		latestState: `{"testField":"test-value-expired"}`,
	}))
	s.collect(e2eMetadataEvent(t, now.Add(-time.Hour), e2eError{
		code:        "E2",
		description: "test recent error",
		payload:     "test-payload-recent",
		latestState: `{"testField":"test-value-recent"}`,
	}))
	status, _ := s.sample("E1", "test expired error")
	require.Equal(t, http.StatusOK, status)

	s.stop()
	s.start()

	assert.Eventually(t, func() bool {
		status, _ := s.sample("E1", "test expired error")
		return status == http.StatusNotFound
	}, 10*time.Second, 100*time.Millisecond)
	status, body := s.sample("E2", "test recent error")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, "test-payload-recent")
}

func TestE2EAuthAndHealth(t *testing.T) {
	s := newE2EServer(t)

	status, _ := s.get("/event-forwarding/v1/errors", e2eToken)
	assert.Equal(t, http.StatusOK, status)
	status, _ = s.get("/event-forwarding/v1/errors", "")
	assert.Equal(t, http.StatusForbidden, status)
	status, _ = s.get("/event-forwarding/v1/errors", "Bearer test-wrong-token")
	assert.Equal(t, http.StatusForbidden, status)
	status, body := s.get("/healthz", "")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "healthy", body)
}
