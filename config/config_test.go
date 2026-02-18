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
	"path/filepath"
	"testing"

	"github.com/snowplow/snowbridge/v3/assets"
	"github.com/snowplow/snowbridge/v3/pkg/failure"
	"github.com/stretchr/testify/assert"
)

func TestNewConfig_NoConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("unexpected error: %q", err.Error())
	}

	assert.Equal(c.Data.Source.Use.Name, "stdin")
	assert.Nil(c.Data.Source.Use.Body)
	assert.Equal(c.Data.Target.Target.Name, "stdout")
	assert.Nil(c.Data.Target.Target.Body)
	assert.Equal(c.Data.FailureTarget.Target.Name, "stdout")
	assert.Nil(c.Data.FailureTarget.Target.Body)
	assert.Equal(c.Data.FailureParser.Format, "snowplow")
	assert.Equal(c.Data.FilterTarget.Target.Name, "silent")
	assert.Nil(c.Data.FilterTarget.Target.Body)
	assert.Equal(c.Data.Sentry.Tags, "{}")
	assert.Equal(c.Data.StatsReceiver.Receiver.Name, "")
	assert.Nil(c.Data.StatsReceiver.Receiver.Body)
	assert.Equal(c.Data.StatsReceiver.TimeoutSec, 1)
	assert.Equal(c.Data.StatsReceiver.BufferSec, 15)
	assert.NotNil(c.Data.Transform)
	assert.Nil(c.Data.Transform.Transformations)
	assert.Equal(c.Data.Transform.WorkerPool, 0)
	assert.Equal(c.Data.LogLevel, "info")
	assert.Equal(c.Data.DisableTelemetry, false)
	assert.Equal(c.Data.License.Accept, false)
	assert.Equal(1000, c.Data.Retry.Transient.Delay)
	assert.Equal(5, c.Data.Retry.Transient.MaxAttempts)
	assert.Equal(20000, c.Data.Retry.Setup.Delay)
}

func TestNewConfig_GetFailureParser(t *testing.T) {
	assert := assert.New(t)

	t.Run("snowplow_format", func(t *testing.T) {
		hclConfig := []byte(`
			failure_parser {
				format = "snowplow"
			}
		`)
		c, err := NewHclConfig(hclConfig, "test.hcl")
		assert.NotNil(c)
		assert.Nil(err)

		parser, err := c.GetFailureParser(500, "testApp", "1.0.0")
		assert.NotNil(parser)
		assert.Nil(err)
		assert.IsType(&failure.SnowplowFailure{}, parser)
	})

	t.Run("event_forwarding_format", func(t *testing.T) {
		hclConfig := []byte(`
			failure_parser {
				format = "event_forwarding"
			}
		`)
		c, err := NewHclConfig(hclConfig, "test.hcl")
		assert.NotNil(c)
		assert.Nil(err)

		parser, err := c.GetFailureParser(500, "testApp", "1.0.0")
		assert.NotNil(parser)
		assert.Nil(err)
		assert.IsType(&failure.EventForwardingFailure{}, parser)
	})

	t.Run("invalid_format", func(t *testing.T) {
		hclConfig := []byte(`
			failure_parser {
				format = "fakeHCL"
			}
		`)
		c, err := NewHclConfig(hclConfig, "test.hcl")
		assert.NotNil(c)
		assert.Nil(err)

		parser, err := c.GetFailureParser(500, "testApp", "1.0.0")
		assert.Nil(parser)
		assert.NotNil(err)
		if err != nil {
			assert.Equal("invalid failure format found; expected one of 'snowplow', 'event_forwarding' and got 'fakeHCL'", err.Error())
		}
	})
}

func TestNewConfig_GetTags(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	tags, err := c.GetTags()
	assert.NotNil(tags)
	assert.Nil(err)

	processID, ok := tags["process_id"]
	assert.NotEqual("", processID)
	assert.True(ok)
	hostname, ok := tags["host"]
	assert.NotEqual("", hostname)
	assert.True(ok)
}

func TestNewConfig_Hcl_invalids(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	c.Data.StatsReceiver.Receiver.Name = "fakeHCL"
	t.Run("invalid_stats_receiver", func(t *testing.T) {
		statsReceiver, err := c.GetObserver("testAppName", "0.0.0", map[string]string{})
		assert.Nil(statsReceiver)
		assert.NotNil(err)
		if err != nil {
			assert.Equal("Invalid stats receiver found; expected one of 'statsd' and got 'fakeHCL'", err.Error())
		}
	})
}

func TestNewConfig_Hcl_NoExt_defaults(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "sentry")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal(true, c.Data.Sentry.Debug)
	assert.Equal("{\"testKey\":\"testValue\"}", c.Data.Sentry.Tags)
	assert.Equal("testDsn", c.Data.Sentry.Dsn)
}

func TestNewConfig_Hcl_defaults(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal("stdin", c.Data.Source.Use.Name)
	assert.Equal("stdout", c.Data.Target.Target.Name)
	assert.Equal("stdout", c.Data.FailureTarget.Target.Name)
	assert.Equal("snowplow", c.Data.FailureParser.Format)
	assert.Equal("{}", c.Data.Sentry.Tags)
	assert.Equal(false, c.Data.Sentry.Debug)
	assert.Equal(1, c.Data.StatsReceiver.TimeoutSec)
	assert.Equal(15, c.Data.StatsReceiver.BufferSec)
	assert.Equal("info", c.Data.LogLevel)
	assert.Equal(1000, c.Data.Retry.Transient.Delay)
	assert.Equal(5, c.Data.Retry.Transient.MaxAttempts)
	assert.Equal(20000, c.Data.Retry.Setup.Delay)
}

func TestNewConfig_Hcl_sentry(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "sentry.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal(true, c.Data.Sentry.Debug)
	assert.Equal("{\"testKey\":\"testValue\"}", c.Data.Sentry.Tags)
	assert.Equal("testDsn", c.Data.Sentry.Dsn)
}

func TestNewConfig_HclTransformationOrder(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "transform-mocked-order.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.NotNil(c.Data.Transform)
	assert.Equal(5, len(c.Data.Transform.Transformations))
	assert.Equal("one", c.Data.Transform.Transformations[0].Name)
	assert.Equal("two", c.Data.Transform.Transformations[1].Name)
	assert.Equal("three", c.Data.Transform.Transformations[2].Name)
	assert.Equal("four", c.Data.Transform.Transformations[3].Name)
	assert.Equal("five", c.Data.Transform.Transformations[4].Name)
}

func TestNewConfig_GetMonitoring(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	monitoring, alertChan, err := c.GetWebhookMonitoring("", "")
	assert.Nil(monitoring)
	assert.Nil(alertChan)
	assert.Nil(err)

	// Should error with invalid endpoint
	c.Data.Monitoring.Webhook.Endpoint = "http:/example.com"
	monitoring, alertChan, err = c.GetWebhookMonitoring("", "")
	assert.Nil(monitoring)
	assert.Nil(alertChan)
	assert.NotNil(err)

	// Should not error with valid endpoint
	c.Data.Monitoring.Webhook.Endpoint = "http://example.com"
	monitoring, alertChan, err = c.GetWebhookMonitoring("", "")
	assert.NotNil(monitoring)
	assert.NotNil(alertChan)
	assert.Nil(err)

	// Should be able to build observer with metadata reporter
	c.Data.Monitoring.MetadataReporter.Endpoint = "http://example.com"
	observer, err := c.GetObserver("", "", map[string]string{})
	assert.NotNil(observer)
	assert.Nil(err)

	// Should fail to build observer with metadata reporter
	c.Data.Monitoring.MetadataReporter.Endpoint = "http:/example.com"
	observer, err = c.GetObserver("", "", map[string]string{})
	assert.Nil(observer)
	assert.NotNil(err)
}
