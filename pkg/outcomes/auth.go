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
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Authorizer decides whether the bearer of an Authorization header may read outcomes
type Authorizer interface {
	Authorize(ctx context.Context, authHeader string) (bool, error)
}

// ConsoleAuthorizer forwards the token to the Console authz API, which validates it
type ConsoleAuthorizer struct {
	url    string
	client *http.Client
	ttl    time.Duration
	now    func() time.Time

	mu      sync.Mutex
	allowed map[[sha256.Size]byte]time.Time
}

// NewConsoleAuthorizer builds a ConsoleAuthorizer for a Console API base URL, e.g. https://console.snowplowanalytics.com
func NewConsoleAuthorizer(baseURL, organizationID, resource, action string) *ConsoleAuthorizer {
	return &ConsoleAuthorizer{
		url:     fmt.Sprintf("%s/api/msc/internal/authz/query/v1/%s/%s/%s", strings.TrimRight(baseURL, "/"), organizationID, resource, action),
		client:  &http.Client{Timeout: 10 * time.Second},
		ttl:     time.Minute,
		now:     time.Now,
		allowed: map[[sha256.Size]byte]time.Time{},
	}
}

// Authorize returns true when Console answers `true`; allowed tokens are cached for a minute
func (a *ConsoleAuthorizer) Authorize(ctx context.Context, authHeader string) (bool, error) {
	if authHeader == "" {
		return false, nil
	}
	key := sha256.Sum256([]byte(authHeader))
	if a.cached(key) {
		return true, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, strings.NewReader("{}"))
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", authHeader)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return false, err
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("authz API returned %d", resp.StatusCode)
	}
	if strings.TrimSpace(string(body)) != "true" {
		return false, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.allowed[key] = a.now().Add(a.ttl)
	return true, nil
}

func (a *ConsoleAuthorizer) cached(key [sha256.Size]byte) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	for k, expiry := range a.allowed {
		if !now.Before(expiry) {
			delete(a.allowed, k)
		}
	}
	_, ok := a.allowed[key]
	return ok
}
