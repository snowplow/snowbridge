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
	assert.Equal(c.Data.Target.Use.Name, "stdout")
	assert.Nil(c.Data.Target.Use.Body)
	assert.Equal(c.Data.FailureTarget.Target.Name, "stdout")
	assert.Nil(c.Data.FailureTarget.Target.Body)
	assert.Equal(c.Data.FailureTarget.Format, "snowplow")
	assert.Equal(c.Data.FilterTarget.Use.Name, "silent")
	assert.Nil(c.Data.FilterTarget.Use.Body)
	assert.Equal(c.Data.Sentry.Tags, "{}")
	assert.Equal(c.Data.StatsReceiver.Receiver.Name, "")
	assert.Nil(c.Data.StatsReceiver.Receiver.Body)
	assert.Equal(c.Data.StatsReceiver.TimeoutSec, 1)
	assert.Equal(c.Data.StatsReceiver.BufferSec, 15)
	assert.Nil(c.Data.Transformations)
	assert.Equal(c.Data.LogLevel, "info")
	assert.Equal(c.Data.DisableTelemetry, false)
	assert.Equal(c.Data.License.Accept, false)
	assert.Equal(1000, c.Data.Retry.Transient.Delay)
	assert.Equal(5, c.Data.Retry.Transient.MaxAttempts)
	assert.Equal(20000, c.Data.Retry.Setup.Delay)
}

func TestNewConfig_InvalidFailureFormat(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	c.Data.FailureTarget.Format = "fakeHCL"
	ft, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.Nil(ft)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Invalid failure format found; expected one of 'snowplow', 'event_forwarding' and got 'fakeHCL'", err.Error())
	}
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

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	c.Data.Target.Use.Name = "fakeHCL"
	c.Data.FailureTarget.Target.Name = "fakeHCL"
	c.Data.StatsReceiver.Receiver.Name = "fakeHCL"
	t.Run("invalid_target", func(t *testing.T) {
		target, err := c.GetTarget()
		assert.Nil(target)
		assert.NotNil(err)
		if err != nil {
			assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fakeHCL'", err.Error())
		}
	})

	t.Run("invalid_failure_target", func(t *testing.T) {
		ftarget, err := c.GetFailureTarget("testAppName", "0.0.0")
		assert.Nil(ftarget)
		assert.NotNil(err)
		if err != nil {
			assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fakeHCL'", err.Error())
		}
	})

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
	assert.Equal("stdout", c.Data.Target.Use.Name)
	assert.Equal("stdout", c.Data.FailureTarget.Target.Name)
	assert.Equal("snowplow", c.Data.FailureTarget.Format)
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

	assert.Equal(5, len(c.Data.Transformations))
	assert.Equal("one", c.Data.Transformations[0].Use.Name)
	assert.Equal("two", c.Data.Transformations[1].Use.Name)
	assert.Equal("three", c.Data.Transformations[2].Use.Name)
	assert.Equal("four", c.Data.Transformations[3].Use.Name)
	assert.Equal("five", c.Data.Transformations[4].Use.Name)
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
