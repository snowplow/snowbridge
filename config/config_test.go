/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package config

import (
	"path/filepath"
	"testing"

	"github.com/snowplow/snowbridge/assets"
	"github.com/stretchr/testify/assert"
)

func TestNewConfig_NoConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.Nil(c)
	if err == nil {
		t.Fatalf("expected a non nil error")
	}

	assert.Equal("configuration file not provided", err.Error())
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
		assert.Equal("Invalid failure format found; expected one of 'snowplow' and got 'fakeHCL'", err.Error())
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
		statsReceiver, err := c.GetObserver(map[string]string{})
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
