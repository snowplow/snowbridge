// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

// The GetSource part needs to move anyway - causes circular dep.
func TestNewConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal("info", c.Data.LogLevel)
	assert.Equal("stdout", c.Data.Target.Use.Name)
	assert.Equal("none", c.Data.Transform.Message)
	assert.Equal("stdin", c.Data.Source.Use.Name)

	// Tests on sources moved to the source package.

	target, err := c.GetTarget()
	assert.NotNil(target)
	assert.Nil(err)

	failureTarget, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.NotNil(failureTarget)
	assert.Nil(err)

	observer, err := c.GetObserver(map[string]string{})
	assert.NotNil(observer)
	assert.Nil(err)
	os.RemoveAll(`tmp_replicator`)
}

func TestNewConfig_FromEnv(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("TARGET_NAME", "kinesis")
	t.Setenv("SOURCE_NAME", "kinesis")

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal("debug", c.Data.LogLevel)
	assert.Equal("kinesis", c.Data.Target.Use.Name)
	assert.Equal("kinesis", c.Data.Source.Use.Name)
}

func TestNewConfig_FromEnvInvalid(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
}

func TestNewConfig_InvalidTarget(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("TARGET_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	source, err := c.GetTarget()
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidFailureTarget(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("FAILURE_TARGET_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	source, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidFailureFormat(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("FAILURE_TARGETS_FORMAT", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	source, err := c.GetFailureTarget("testAppName", "0.0.0")
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid failure format found; expected one of 'snowplow' and got 'fake'", err.Error())
}

func TestNewConfig_InvalidStatsReceiver(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("STATS_RECEIVER_NAME", "fake")

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	source, err := c.GetObserver(map[string]string{})
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid stats receiver found; expected one of 'statsd' and got 'fake'", err.Error())
}

func TestNewConfig_GetTags(t *testing.T) {
	assert := assert.New(t)

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

	filename := filepath.Join("test-fixtures", "invalids.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	t.Run("invalid_target", func(t *testing.T) {
		target, err := c.GetTarget()
		assert.Nil(target)
		assert.NotNil(err)
		assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fakeHCL'", err.Error())
	})

	t.Run("invalid_failure_target", func(t *testing.T) {
		ftarget, err := c.GetFailureTarget("testAppName", "0.0.0")
		assert.Nil(ftarget)
		assert.NotNil(err)
		assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fakeHCL'", err.Error())
	})

}

func TestNewConfig_Hcl_defaults(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join("test-fixtures", "empty.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal(c.Data.Source.Use.Name, "stdin")
	assert.Equal(c.Data.Target.Use.Name, "stdout")
	assert.Equal(c.Data.FailureTarget.Target.Name, "stdout")
	assert.Equal(c.Data.FailureTarget.Format, "snowplow")
	assert.Equal(c.Data.Sentry.Tags, "{}")
	assert.Equal(c.Data.Sentry.Debug, false)
	assert.Equal(c.Data.StatsReceiver.TimeoutSec, 1)
	assert.Equal(c.Data.StatsReceiver.BufferSec, 15)
	assert.Equal(c.Data.Transform.Message, "none")
	assert.Equal(c.Data.LogLevel, "info")
}

func TestNewConfig_Hcl_sentry(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join("test-fixtures", "sentry.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal(c.Data.Sentry.Debug, true)
	assert.Equal(c.Data.Sentry.Tags, "{\"testKey\":\"testValue\"}")
	assert.Equal(c.Data.Sentry.Dsn, "testDsn")
}

func TestDefaultTransformation(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", "")
	t.Setenv("MESSAGE_TRANSFORMATION", "")

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal("none", c.Data.Transform.Message)
	assert.Equal("none", c.ProvideTransformMessage())
	assert.Equal("", c.ProvideTransformLayerName())
}

func TestTransformationProviderImplementation(t *testing.T) {
	testFixPath := "./test-fixtures"
	testCases := []struct {
		File      string
		Plug      Pluggable
		Message   string
		LayerName string
	}{
		{
			File:      "transform-lua-simple.hcl",
			Plug:      transform.LuaLayer().(Pluggable),
			Message:   "lua:fun",
			LayerName: "lua",
		},
		{
			File:      "transform-js-simple.hcl",
			Plug:      transform.JSLayer().(Pluggable),
			Message:   "js:fun",
			LayerName: "js",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			configFile := filepath.Join(testFixPath, tt.File)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", configFile)

			c, err := NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			assert.Equal(tt.Message, c.ProvideTransformMessage())
			assert.Equal(tt.LayerName, c.ProvideTransformLayerName())

			component, err := c.ProvideTransformComponent(tt.Plug)
			assert.Nil(err)
			assert.NotNil(component)

		})
	}
}
