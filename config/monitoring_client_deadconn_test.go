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

package config

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/v6/pkg/models"
	"github.com/snowplow/snowbridge/v6/pkg/monitoring"
	"github.com/stretchr/testify/assert"
)

type blackholeConn struct {
	net.Conn
	mu         sync.Mutex
	blackholed bool
	closed     chan struct{}
	closeOnce  sync.Once
}

func newBlackholeConn(c net.Conn) *blackholeConn {
	return &blackholeConn{Conn: c, closed: make(chan struct{})}
}

func (c *blackholeConn) blackhole() {
	c.mu.Lock()
	c.blackholed = true
	c.mu.Unlock()
}

func (c *blackholeConn) isBlackholed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.blackholed
}

func (c *blackholeConn) Read(b []byte) (int, error) {
	if c.isBlackholed() {
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Read(b)
}

func (c *blackholeConn) Write(b []byte) (int, error) {
	if c.isBlackholed() {
		return len(b), nil
	}
	return c.Conn.Write(b)
}

func (c *blackholeConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type dialHarness struct {
	backendAddr string
	mu          sync.Mutex
	conns       []*blackholeConn
}

func (h *dialHarness) dialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", h.backendAddr)
	if err != nil {
		return nil, err
	}
	c := newBlackholeConn(raw)

	h.mu.Lock()
	h.conns = append(h.conns, c)
	h.mu.Unlock()

	return c, nil
}

func (h *dialHarness) blackholeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, c := range h.conns {
		c.blackhole()
	}
}

type serverObservations struct {
	mu        sync.Mutex
	requests  int
	lastProto string
}

func (o *serverObservations) record(proto string) {
	o.mu.Lock()
	o.requests++
	o.lastProto = proto
	o.mu.Unlock()
}

func (o *serverObservations) requestCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.requests
}

func (o *serverObservations) protocol() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.lastProto
}

func newCountingTLSServer(t *testing.T) (*httptest.Server, *serverObservations) {
	t.Helper()
	obs := &serverObservations{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		obs.record(r.Proto)
		w.WriteHeader(http.StatusOK)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, obs
}

func TestMonitoringClientRecoversFromDeadConnection(t *testing.T) {
	assert := assert.New(t)

	server, obs := newCountingTLSServer(t)
	harness := &dialHarness{backendAddr: server.Listener.Addr().String()}

	// Route the client's connections through a dialer we can silently blackhole.
	client := newMonitoringHTTPClient(300 * time.Millisecond)
	transport := client.Transport.(*http.Transport)
	transport.DialContext = harness.dialContext
	// Keep the client's pinned ALPN (http/1.1); only trust the test server's cert.
	transport.TLSClientConfig.InsecureSkipVerify = true

	reporter := monitoring.NewMetadataReporter("snowbridge", "test", client, server.URL, nil)

	// Warm up: establish a working connection and confirm the negotiated protocol.
	reporter.Send(&models.ObserverBuffer{}, time.Now(), time.Now())
	assert.Positive(obs.requestCount())
	assert.Equal("HTTP/1.1", obs.protocol())

	// Kill the established connection at the TCP layer, with no FIN/RST.
	harness.blackholeAll()

	// The reporter must recover on a fresh connection, not reuse the dead one.
	before := obs.requestCount()
	reporter.Send(&models.ObserverBuffer{}, time.Now(), time.Now())

	assert.Greater(obs.requestCount(), before)
	assert.Equal("HTTP/1.1", obs.protocol())
}
