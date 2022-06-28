// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	
	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
)

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

	defer os.Unsetenv("LOG_LEVEL")
	defer os.Unsetenv("TARGET_NAME")
	defer os.Unsetenv("SOURCE_NAME")

	os.Setenv("LOG_LEVEL", "debug")
	os.Setenv("TARGET_NAME", "kinesis")
	os.Setenv("SOURCE_NAME", "kinesis")

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

	defer os.Unsetenv("STATS_RECEIVER_TIMEOUT_SEC")

	os.Setenv("STATS_RECEIVER_TIMEOUT_SEC", "debug")

	c, err := NewConfig()
	assert.Nil(c)
	assert.NotNil(err)
}

func TestNewConfig_InvalidTarget(t *testing.T) {
	assert := assert.New(t)

	defer os.Unsetenv("TARGET_NAME")

	os.Setenv("TARGET_NAME", "fake")

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

	defer os.Unsetenv("FAILURE_TARGET_NAME")

	os.Setenv("FAILURE_TARGET_NAME", "fake")

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

	defer os.Unsetenv("FAILURE_TARGETS_FORMAT")

	os.Setenv("FAILURE_TARGETS_FORMAT", "fake")

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

	defer os.Unsetenv("STATS_RECEIVER_NAME")

	os.Setenv("STATS_RECEIVER_NAME", "fake")

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

type expectedMessages struct {
	Before []*models.Message
	After  []*models.Message
}

func TestEnginesAndTransformations(t *testing.T) {
	var messageJSCompileErr = &models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}
	messageJSCompileErr.SetError(errors.New(`failed initializing JavaScript runtime: "could not assert as function: \"main\""`))

	testFixPath := "./test-fixtures"
	testCases := []struct {
		Description        string
		File               string
		ExpectedEngines    []engine.Engine
		ExpectedTransforms []transformconfig.Transformation
		ExpectedMessages   expectedMessages
	}{
		{
			Description: "simple engine and transform success",
			File:        "transform-js-simple.hcl",
			ExpectedEngines: []engine.Engine{
				&engine.JSEngine{
					Name: "test-engine",
				}},
			ExpectedTransforms: []transformconfig.Transformation{
				{
					Name:       "js",
					EngineName: "test-engine",
				}},
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: "simple engine and transform with js compile error",
			File:        "transform-js-error.hcl",
			ExpectedEngines: []engine.Engine{
				&engine.JSEngine{
					Name: "test-engine",
				}},
			ExpectedTransforms: []transformconfig.Transformation{
				{
					Name:       "js",
					EngineName: "test-engine",
				}},
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{messageJSCompileErr},
			},
		},
		{
			Description: "extended engine and transform success",
			File:        "transform-js-extended.hcl",
			ExpectedEngines: []engine.Engine{
				&engine.JSEngine{
					Name:       "test-engine",
					RunTimeout: 20,
					SpMode:     true,
				}},
			ExpectedTransforms: []transformconfig.Transformation{
				{
					Name:       "js",
					EngineName: "test-engine",
				}},
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowJSON1After,
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: `mixed engines success`,
			File:        "transform-mixed.hcl",
			ExpectedEngines: []engine.Engine{
				&engine.JSEngine{
					Name: "engine1",
				},
				&engine.JSEngine{
					Name: "engine2",
				},
				&engine.LuaEngine{
					Name: "engine3",
				},
			},
			ExpectedTransforms: []transformconfig.Transformation{
				{
					Name:       "js",
					EngineName: "engine1",
				},
				{
					Name:       "js",
					EngineName: "engine2",
				},
				{
					Name:       "lua",
					EngineName: "engine3",
				}},
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowJSON1Mixed,
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: `mixed engines with error`,
			File:        "transform-mixed-error.hcl",
			ExpectedEngines: []engine.Engine{
				&engine.JSEngine{
					Name: "engine1",
				},
				&engine.JSEngine{
					Name: "engine2",
				},
				&engine.LuaEngine{
					Name: "engine3",
				},
			},
			ExpectedTransforms: []transformconfig.Transformation{
				{
					Name:       "js",
					EngineName: "engine1",
				},
				{
					Name:       "js",
					EngineName: "engine2",
				},
				{
					Name:       "lua",
					EngineName: "engine3",
				}},
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{messageJSCompileErr},
			},
		},
		{
			Description: `mixed with filter success`,
			File:        "transform-mixed-filtered.hcl",
			ExpectedEngines: []engine.Engine{
				&engine.JSEngine{
					Name: "engine1",
				},
				&engine.JSEngine{
					Name: "engine2",
				},
			},
			ExpectedTransforms: []transformconfig.Transformation{
				{
					Name:       "js",
					EngineName: "engine1",
				},
				{
					Name:  "spEnrichedFilter",
					Field: "app_id",
					Regex: "again",
				},
				{
					Name:       "js",
					EngineName: "engine2",
				},
			},
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Description, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(testFixPath, tt.File)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

			c, err := NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			// get engines, check that all of them have been initiated
			engines, err := c.GetEngines()
			assert.NotNil(engines)
			assert.Nil(err)

			assert.Equal(len(tt.ExpectedEngines), len(engines))

			for idx, eng := range engines {
				assert.Equal(tt.ExpectedEngines[idx].GetName(), eng.GetName())
			}

			// get transformations, and run the transformations on the expected messages
			tr, err := c.GetTransformations(engines)
			assert.NotNil(tr)
			assert.Nil(err)

			result := tr(tt.ExpectedMessages.Before)
			assert.NotNil(result)
			assert.Equal(int(result.ResultCount+result.FilteredCount+result.InvalidCount), len(tt.ExpectedMessages.After))

			// check result for successfully transformed messages
			for idx, resultMessage := range result.Result {
				assert.Equal(resultMessage.Data, tt.ExpectedMessages.After[idx].Data)
			}

			// check errors for invalid messages
			for idx, resultMessage := range result.Invalid {
				assert.Equal(resultMessage.GetError(), tt.ExpectedMessages.After[idx].GetError())
			}

			// check result for transformed messages in case of filtered results
			if result.FilteredCount != 0 {
				assert.NotNil(result.Filtered)
				for idx, resultMessage := range result.Filtered {
					assert.Equal(resultMessage.Data, tt.ExpectedMessages.After[idx].Data)
				}
			}
		})
	}
}

var snowplowTsv1 = []byte(`test-data1	pc	2019-05-10 14:40:37.436	2019-05-10 14:40:35.972	2019-05-10 14:40:35.551	unstruct	e9234345-f042-46ad-b1aa-424464066a33			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				d26822f5-52cc-4292-8f77-14ef6b7a27e2																																									{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0","data":{"sku":"item41","quantity":2,"unitPrice":32.4,"currency":"GBP"}}}																			python-requests/2.21.0																																										2019-05-10 14:40:35.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:35.972	com.snowplowanalytics.snowplow	add_to_cart	jsonschema	1-0-0		`)
var snowplowJSON1 = []byte(`{"app_id":"test-data1","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user\u003cbuilt-in function input\u003e","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// snowplowJSON1 with changed app_id
var snowplowJSON1After = []byte(`{"app_id":"changed","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// snowplowJSON1 with 3 transformations applied
var snowplowJSON1Mixed = []byte(`Hello:{"app_id":"again","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)
