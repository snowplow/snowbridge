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

func TestNewConfig(t *testing.T) {
	assert := assert.New(t)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal("info", c.Data.LogLevel)
	assert.Equal("stdout", c.Data.Target.Use.Name)
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
}

func TestNewConfig_FromEnv(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("TARGET_NAME", "kinesis")
	t.Setenv("SOURCE_NAME", "kinesis")
	t.Setenv("TRANSFORM_CONFIG_B64", `dHJhbnNmb3JtIHsKICB1c2UgImpzIiB7CiAgICAvLyBjaGFuZ2VzIGFwcF9pZCB0byAiMSIKICAgIHNvdXJjZV9iNjQgPSAiWm5WdVkzUnBiMjRnYldGcGJpaDRLU0I3Q2lBZ0lDQjJZWElnYW5OdmJrOWlhaUE5SUVwVFQwNHVjR0Z5YzJVb2VDNUVZWFJoS1RzS0lDQWdJR3B6YjI1UFltcGJJbUZ3Y0Y5cFpDSmRJRDBnSWpFaU93b2dJQ0FnY21WMGRYSnVJSHNLSUNBZ0lDQWdJQ0JFWVhSaE9pQktVMDlPTG5OMGNtbHVaMmxtZVNocWMyOXVUMkpxS1FvZ0lDQWdmVHNLZlE9PSIKICB9Cn0KCnRyYW5zZm9ybSB7CiAgdXNlICJqcyIgewogICAgLy8gaWYgYXBwX2lkID09ICIxIiBpdCBpcyBjaGFuZ2VkIHRvICIyIgogICAgc291cmNlX2I2NCA9ICJablZ1WTNScGIyNGdiV0ZwYmloNEtTQjdDaUFnSUNCMllYSWdhbk52Yms5aWFpQTlJRXBUVDA0dWNHRnljMlVvZUM1RVlYUmhLVHNLSUNBZ0lHbG1JQ2hxYzI5dVQySnFXeUpoY0hCZmFXUWlYU0E5UFNBaU1TSXBJSHNLSUNBZ0lDQWdJQ0JxYzI5dVQySnFXeUpoY0hCZmFXUWlYU0E5SUNJeUlnb2dJQ0FnZlFvZ0lDQWdjbVYwZFhKdUlIc0tJQ0FnSUNBZ0lDQkVZWFJoT2lCS1UwOU9Mbk4wY21sdVoybG1lU2hxYzI5dVQySnFLUW9nSUNBZ2ZUc0tmUT09IgogIH0KfQoKdHJhbnNmb3JtIHsKICB1c2UgImpzIiB7CiAgICAvLyBpZiBhcHBfaWQgPT0gIjIiIGl0IGlzIGNoYW5nZWQgdG8gIjMiCiAgICBzb3VyY2VfYjY0ID0gIlpuVnVZM1JwYjI0Z2JXRnBiaWg0S1NCN0NpQWdJQ0IyWVhJZ2FuTnZiazlpYWlBOUlFcFRUMDR1Y0dGeWMyVW9lQzVFWVhSaEtUc0tJQ0FnSUdsbUlDaHFjMjl1VDJKcVd5SmhjSEJmYVdRaVhTQTlQU0FpTWlJcElIc0tJQ0FnSUNBZ0lDQnFjMjl1VDJKcVd5SmhjSEJmYVdRaVhTQTlJQ0l6SWdvZ0lDQWdmUW9nSUNBZ2NtVjBkWEp1SUhzS0lDQWdJQ0FnSUNCRVlYUmhPaUJLVTA5T0xuTjBjbWx1WjJsbWVTaHFjMjl1VDJKcUtRb2dJQ0FnZlRzS2ZRPT0iCiAgfQp9`)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	assert.Equal("debug", c.Data.LogLevel)
	assert.Equal("kinesis", c.Data.Target.Use.Name)
	assert.Equal("kinesis", c.Data.Source.Use.Name)
	for _, transf := range c.Data.Transformations {
		assert.Equal("js", transf.Use.Name)
	}
}

func TestNewConfig_FromEnvInvalid(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error parsing env config: env: parse error on field \"TimeoutSec\" of type \"int\": strconv.ParseInt: parsing \"debug\": invalid syntax", err.Error())
	}
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
	if err != nil {
		assert.Equal("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fake'", err.Error())
	}
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
	if err != nil {
		assert.Equal("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got 'fake'", err.Error())
	}
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
	if err != nil {
		assert.Equal("Invalid failure format found; expected one of 'snowplow' and got 'fake'", err.Error())
	}
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
	if err != nil {
		assert.Equal("Invalid stats receiver found; expected one of 'statsd' and got 'fake'", err.Error())
	}
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

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "invalids.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

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

func TestNewConfig_InvalidTransformationB64(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("TRANSFORM_CONFIG_B64", `fdssdnpfdspnm`)

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error decoding b64 data from TRANSFORM_CONFIG_B64 env var: illegal base64 data at input byte 12", err.Error())
	}

}

func TestNewConfig_UnparseableTransformationB64(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("TRANSFORM_CONFIG_B64", `dHJhbnNmb3JtIHsKICB1c2UgImpzIiB7CiAgICAvLyBjaGFuZ2VzIGFwcF9pZCB0byAiMSIKICAgIHNvdXJjZV9iNjQgPSAiWm5WdVkzUnBiMjRnYldGcGJpaDRLU0I3Q2lBZ0lDQjJZWElnYW5OdmJrOWlhaUE5SUVwVFQwNHVjR0Z5YzJVb2VDNUVZWFJoS1RzS0lDQWdJR3B6YjI1UFltcGJJbUZ3Y0Y5cFpDSmRJRDBnSWpFaU93b2dJQ0FnY21WMGRYSnVJSHNLSUNBZ0lDQWdJQ0JFWVhSaE9pQktVMDlPTG5OMGNtbHVaMmxtZVNocWMyOXVUMkpxS1FvZ0lDQWdmVHNLZlE9PSIKICB9Cn0KCnRyYW5zZm9ybSB7CiAgdXNlICJqcyIgewogICAgLy8gaWYgYXBwX2lkID09ICIxIiBpdCBpcyBjaGFuZ2VkIHRvICIyIgogICAgc291cmNlX2I2NCA9ICJablZ1WTNScGIyNGdiV0ZwYmloNEtTQjdDaUFnSUNCMllYSWdhbk52Yms5aWFpQTlJRXBUVDA0dWNHRnljMlVvZUM1RVlYUmhLVHNLSUNBZ0lHbG1JQ2hxYzI5dVQySnFXeUpoY0hCZmFXUWlYU0E5UFNBaU1TSXBJSHNLSUNBZ0lDQWdJQ0JxYzI5dVQySnFXeUpoY0hCZmFXUWlYU0E5SUNJeUlnb2dJQ0FnZlFvZ0lDQWdjbVYwZFhKdUlIc0tJQ0FnSUNBZ0lDQkVZWFJoT2lCS1UwOU9Mbk4wY21sdVoybG1lU2hxYzI5dVQySnFLUW9nSUNBZ2ZUc0tmUT09IgoKfQoKdHJhbnNmb3JtIHsKICB1c2UgImpzIiB7CiAgICAvLyBpZiBhcHBfaWQgPT0gIjIiIGl0IGlzIGNoYW5nZWQgdG8gIjMiCiAgICBzb3VyY2VfYjY0ID0gIlpuVnVZM1JwYjI0Z2JXRnBiaWg0S1NCN0NpQWdJQ0IyWVhJZ2FuTnZiazlpYWlBOUlFcFRUMDR1Y0dGeWMyVW9lQzVFWVhSaEtUc0tJQ0FnSUdsbUlDaHFjMjl1VDJKcVd5SmhjSEJmYVdRaVhTQTlQU0FpTWlJcElIc0tJQ0FnSUNBZ0lDQnFjMjl1VDJKcVd5SmhjSEJmYVdRaVhTQTlJQ0l6SWdvZ0lDQWdmUW9nSUNBZ2NtVjBkWEp1SUhzS0lDQWdJQ0FnSUNCRVlYUmhPaUJLVTA5T0xuTjBjbWx1WjJsbWVTaHFjMjl1VDJKcUtRb2dJQ0FnZlRzS2ZRPT0iCiAgfQp9`)

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error creating config fom provided TRANSFORM_CONFIG_B64: :8,11-12: Unclosed configuration block; There is no closing brace for this block before the end of the file. This may be caused by incorrect brace nesting elsewhere in this file.", err.Error())
	}
}
