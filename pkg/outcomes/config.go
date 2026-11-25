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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config configures the outcomes server
type Config struct {
	ListenAddr     string
	IngestAddr     string
	PostgresDSN    string
	ConsoleDomain  string
	OrganizationID string
	AuthzResource  string
	AuthzAction    string
	CORSOrigin     string
	TTLDays        int
}

// ConfigFromEnv builds the outcomes server config from OUTCOMES_* environment variables.
// Every setting except the two listen addresses is required.
func ConfigFromEnv() (*Config, error) {
	var missing []string
	required := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}

	cfg := &Config{
		ListenAddr:     envOr("OUTCOMES_LISTEN_ADDR", ":8080"),
		IngestAddr:     envOr("OUTCOMES_INGEST_ADDR", ":8081"),
		PostgresDSN:    required("OUTCOMES_PG_DSN"),
		ConsoleDomain:  required("OUTCOMES_CONSOLE_DOMAIN"),
		OrganizationID: required("OUTCOMES_ORG_ID"),
		AuthzResource:  required("OUTCOMES_AUTHZ_RESOURCE"),
		AuthzAction:    required("OUTCOMES_AUTHZ_ACTION"),
		CORSOrigin:     required("OUTCOMES_CORS_ORIGIN"),
	}
	ttlRaw := required("OUTCOMES_TTL_DAYS")

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	ttl, err := strconv.Atoi(ttlRaw)
	if err != nil || ttl < 1 {
		return nil, errors.New("OUTCOMES_TTL_DAYS must be a whole number of days, at least 1")
	}
	cfg.TTLDays = ttl

	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
