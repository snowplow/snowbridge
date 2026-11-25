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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var requiredOutcomesEnv = map[string]string{
	"OUTCOMES_PG_DSN":         "postgres://db:5432/outcomes",
	"OUTCOMES_CONSOLE_DOMAIN": "console.example.com",
	"OUTCOMES_ORG_ID":         "org-123",
	"OUTCOMES_AUTHZ_RESOURCE": "test-resource",
	"OUTCOMES_AUTHZ_ACTION":   "test-action",
	"OUTCOMES_CORS_ORIGIN":    "https://console.example.com",
	"OUTCOMES_TTL_DAYS":       "14",
}

func setOutcomesEnv(t *testing.T, except ...string) {
	t.Helper()
	skip := map[string]bool{}
	for _, k := range except {
		skip[k] = true
	}
	t.Setenv("OUTCOMES_LISTEN_ADDR", "")
	t.Setenv("OUTCOMES_INGEST_ADDR", "")
	for k, v := range requiredOutcomesEnv {
		if skip[k] {
			t.Setenv(k, "")
			continue
		}
		t.Setenv(k, v)
	}
}

func TestConfigFromEnv_AllSet(t *testing.T) {
	setOutcomesEnv(t)
	t.Setenv("OUTCOMES_LISTEN_ADDR", ":9090")
	t.Setenv("OUTCOMES_INGEST_ADDR", ":9091")

	cfg, err := ConfigFromEnv()
	require.NoError(t, err)
	assert.Equal(t, &Config{
		ListenAddr:     ":9090",
		IngestAddr:     ":9091",
		PostgresDSN:    "postgres://db:5432/outcomes",
		ConsoleDomain:  "console.example.com",
		OrganizationID: "org-123",
		AuthzResource:  "test-resource",
		AuthzAction:    "test-action",
		CORSOrigin:     "https://console.example.com",
		TTLDays:        14,
	}, cfg)
}

func TestConfigFromEnv_EachRequiredMissing(t *testing.T) {
	for key := range requiredOutcomesEnv {
		t.Run(key, func(t *testing.T) {
			setOutcomesEnv(t, key)

			cfg, err := ConfigFromEnv()
			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), key)
		})
	}
}

func TestConfigFromEnv_SeveralMissing(t *testing.T) {
	missing := []string{"OUTCOMES_PG_DSN", "OUTCOMES_AUTHZ_RESOURCE", "OUTCOMES_TTL_DAYS"}
	setOutcomesEnv(t, missing...)

	cfg, err := ConfigFromEnv()
	require.Error(t, err)
	assert.Nil(t, cfg)
	for _, key := range missing {
		assert.Contains(t, err.Error(), key)
	}
	assert.NotContains(t, err.Error(), "OUTCOMES_ORG_ID")
}

func TestConfigFromEnv_BadTTL(t *testing.T) {
	for _, ttl := range []string{"abc", "0"} {
		t.Run(ttl, func(t *testing.T) {
			setOutcomesEnv(t)
			t.Setenv("OUTCOMES_TTL_DAYS", ttl)

			cfg, err := ConfigFromEnv()
			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), "OUTCOMES_TTL_DAYS")
		})
	}
}

func TestConfigFromEnv_DefaultPorts(t *testing.T) {
	setOutcomesEnv(t)

	cfg, err := ConfigFromEnv()
	require.NoError(t, err)
	assert.Equal(t, ":8080", cfg.ListenAddr)
	assert.Equal(t, ":8081", cfg.IngestAddr)
}
