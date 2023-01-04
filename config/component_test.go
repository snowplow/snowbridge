//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package config

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/pkg/statsreceiver"
	"github.com/snowplow/snowbridge/pkg/target"
)

func TestCreateTargetComponentHCL(t *testing.T) {
	testCases := []struct {
		File     string
		Plug     Pluggable
		Expected interface{}
	}{
		{
			File: "target-sqs.hcl",
			Plug: testSQSTargetAdapter(testSQSTargetFunc),
			Expected: &target.SQSTargetConfig{
				QueueName: "testQueue",
				Region:    "eu-test-1",
				RoleARN:   "xxx-test-role-arn",
			},
		},
		{
			File: "target-eventhub-simple.hcl",
			Plug: testEventHubTargetAdapter(testEventHubTargetFunc),
			Expected: &target.EventHubConfig{
				EventHubNamespace:       "testNamespace",
				EventHubName:            "testName",
				MaxAutoRetries:          1,
				MessageByteLimit:        1048576,
				ChunkByteLimit:          1048576,
				ChunkMessageLimit:       500,
				ContextTimeoutInSeconds: 20,
				BatchByteLimit:          1048576,
				SetEHPartitionKey:       true,
			},
		},
		{
			File: "target-eventhub-extended.hcl",
			Plug: testEventHubTargetAdapter(testEventHubTargetFunc),
			Expected: &target.EventHubConfig{
				EventHubNamespace:       "testNamespace",
				EventHubName:            "testName",
				MaxAutoRetries:          2,
				MessageByteLimit:        1000000,
				ChunkByteLimit:          1000000,
				ChunkMessageLimit:       501,
				ContextTimeoutInSeconds: 21,
				BatchByteLimit:          1000000,
				SetEHPartitionKey:       false,
			},
		},
		{
			File: "target-http-simple.hcl",
			Plug: testHTTPTargetAdapter(testHTTPTargetFunc),
			Expected: &target.HTTPTargetConfig{
				HTTPURL:                 "testUrl",
				ByteLimit:               1048576,
				RequestTimeoutInSeconds: 5,
				ContentType:             "application/json",
				Headers:                 "",
				BasicAuthUsername:       "",
				BasicAuthPassword:       "",
				CertFile:                "",
				KeyFile:                 "",
				CaFile:                  "",
				SkipVerifyTLS:           false,
			},
		},
		{
			File: "target-http-extended.hcl",
			Plug: testHTTPTargetAdapter(testHTTPTargetFunc),
			Expected: &target.HTTPTargetConfig{
				HTTPURL:                 "testUrl",
				ByteLimit:               1000000,
				RequestTimeoutInSeconds: 2,
				ContentType:             "test/test",
				Headers:                 "{\"Accept-Language\":\"en-US\"}",
				BasicAuthUsername:       "testUsername",
				BasicAuthPassword:       "testPass",
				CertFile:                "myLocalhost.crt",
				KeyFile:                 "MyLocalhost.key",
				CaFile:                  "myRootCA.crt",
				SkipVerifyTLS:           true,
			},
		},
		{
			File: "target-kafka-simple.hcl",
			Plug: testKafkaTargetAdapter(testKafkaTargetFunc),
			Expected: &target.KafkaConfig{
				Brokers:        "testBrokers",
				TopicName:      "testTopic",
				TargetVersion:  "",
				MaxRetries:     10,
				ByteLimit:      1048576,
				Compress:       false,
				WaitForAll:     false,
				Idempotent:     false,
				EnableSASL:     false,
				SASLUsername:   "",
				SASLPassword:   "",
				SASLAlgorithm:  "sha512",
				CertFile:       "",
				KeyFile:        "",
				CaFile:         "",
				SkipVerifyTLS:  false,
				ForceSync:      false,
				FlushFrequency: 0,
				FlushMessages:  0,
				FlushBytes:     0,
			},
		},
		{
			File: "target-kafka-extended.hcl",
			Plug: testKafkaTargetAdapter(testKafkaTargetFunc),
			Expected: &target.KafkaConfig{
				Brokers:        "testBrokers",
				TopicName:      "testTopic",
				TargetVersion:  "1.2.3",
				MaxRetries:     11,
				ByteLimit:      1000000,
				Compress:       true,
				WaitForAll:     true,
				Idempotent:     true,
				EnableSASL:     true,
				SASLUsername:   "testUsername",
				SASLPassword:   "testPass",
				SASLAlgorithm:  "sha256",
				CertFile:       "myLocalhost.crt",
				KeyFile:        "MyLocalhost.key",
				CaFile:         "myRootCA.crt",
				SkipVerifyTLS:  true,
				ForceSync:      true,
				FlushFrequency: 2,
				FlushMessages:  2,
				FlushBytes:     2,
			},
		},
		{
			File: "target-kinesis.hcl",
			Plug: testKinesisTargetAdapter(testKinesisTargetFunc),
			Expected: &target.KinesisTargetConfig{
				StreamName: "testStream",
				Region:     "eu-test-1",
				RoleARN:    "xxx-test-role-arn",
			},
		},
		{
			File: "target-pubsub.hcl",
			Plug: testPubSubTargetAdapter(testPubSubTargetFunc),
			Expected: &target.PubSubTargetConfig{
				ProjectID: "testId",
				TopicName: "testTopic",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", tt.File)
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			c, err := NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			use := c.Data.Target.Use
			decoderOpts := &DecoderOptions{
				Input: use.Body,
			}

			result, err := c.CreateComponent(tt.Plug, decoderOpts)
			assert.NotNil(result)
			assert.Nil(err)

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestCreateFailureTargetComponentENV(t *testing.T) {
	testCase := struct {
		Name     string
		Plug     Pluggable
		Expected interface{}
	}{
		Name: "test_failure_target_kafka_extended_env",
		Plug: testKafkaTargetAdapter(testKafkaTargetFunc),
		Expected: &target.KafkaConfig{
			Brokers:        "testBrokers",
			TopicName:      "testTopic",
			TargetVersion:  "1.2.3",
			MaxRetries:     11,
			ByteLimit:      1000000,
			Compress:       true,
			WaitForAll:     true,
			Idempotent:     true,
			EnableSASL:     true,
			SASLUsername:   "testUsername",
			SASLPassword:   "testPass",
			SASLAlgorithm:  "sha256",
			CertFile:       "test/certfile.crt",
			KeyFile:        "test/keyfile.key",
			CaFile:         "test/cafile.crt",
			SkipVerifyTLS:  true,
			ForceSync:      true,
			FlushFrequency: 2,
			FlushMessages:  2,
			FlushBytes:     2,
		},
	}

	t.Run(testCase.Name, func(t *testing.T) {
		assert := assert.New(t)
		t.Setenv("SNOWBRIDGE_CONFIG_FILE", "")
		t.Setenv("FAILURE_TARGET_NAME", "kafka")
		t.Setenv("FAILURE_TARGET_KAFKA_BROKERS", "testBrokers")
		t.Setenv("FAILURE_TARGET_KAFKA_TOPIC_NAME", "testTopic")
		t.Setenv("FAILURE_TARGET_KAFKA_TARGET_VERSION", "1.2.3")
		t.Setenv("FAILURE_TARGET_KAFKA_MAX_RETRIES", "11")
		t.Setenv("FAILURE_TARGET_KAFKA_BYTE_LIMIT", "1000000")
		t.Setenv("FAILURE_TARGET_KAFKA_COMPRESS", "true")
		t.Setenv("FAILURE_TARGET_KAFKA_WAIT_FOR_ALL", "true")
		t.Setenv("FAILURE_TARGET_KAFKA_IDEMPOTENT", "true")
		t.Setenv("FAILURE_TARGET_KAFKA_ENABLE_SASL", "true")
		t.Setenv("FAILURE_TARGET_KAFKA_SASL_USERNAME", "testUsername")
		t.Setenv("FAILURE_TARGET_KAFKA_SASL_PASSWORD", "testPass")
		t.Setenv("FAILURE_TARGET_KAFKA_SASL_ALGORITHM", "sha256")
		t.Setenv("FAILURE_TARGET_KAFKA_TLS_CERT_FILE", "test/certfile.crt")
		t.Setenv("FAILURE_TARGET_KAFKA_TLS_KEY_FILE", "test/keyfile.key")
		t.Setenv("FAILURE_TARGET_KAFKA_TLS_CA_FILE", "test/cafile.crt")
		t.Setenv("FAILURE_TARGET_KAFKA_TLS_SKIP_VERIFY_TLS", "true")
		t.Setenv("FAILURE_TARGET_KAFKA_FORCE_SYNC_PRODUCER", "true")
		t.Setenv("FAILURE_TARGET_KAFKA_FLUSH_FREQUENCY", "2")
		t.Setenv("FAILURE_TARGET_KAFKA_FLUSH_MESSAGES", "2")
		t.Setenv("FAILURE_TARGET_KAFKA_FLUSH_BYTES", "2")

		c, err := NewConfig()
		assert.NotNil(c)
		if err != nil {
			t.Fatalf("function NewConfig failed with error: %q", err.Error())
		}

		assert.Equal("kafka", c.Data.FailureTarget.Target.Name)
		decoderOpts := &DecoderOptions{
			Prefix: "FAILURE_",
		}

		result, err := c.CreateComponent(testCase.Plug, decoderOpts)
		assert.NotNil(result)
		assert.Nil(err)

		if !reflect.DeepEqual(result, testCase.Expected) {
			t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
				spew.Sdump(result),
				spew.Sdump(testCase.Expected))
		}
	})
}

func TestCreateObserverComponentHCL(t *testing.T) {
	testCases := []struct {
		File     string
		Plug     Pluggable
		Expected interface{}
	}{
		{
			File: "observer.hcl",
			Plug: testStatsDAdapter(testStatsDFunc),
			Expected: &statsreceiver.StatsDStatsReceiverConfig{
				Address: "test.localhost",
				Prefix:  "snowplow.test",
				Tags:    "{\"testKey\": \"testValue\"}",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", tt.File)
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			c, err := NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			assert.Equal(2, c.Data.StatsReceiver.TimeoutSec)
			assert.Equal(20, c.Data.StatsReceiver.BufferSec)

			use := c.Data.StatsReceiver.Receiver
			decoderOpts := &DecoderOptions{
				Input: use.Body,
			}

			result, err := c.CreateComponent(tt.Plug, decoderOpts)
			assert.NotNil(result)
			assert.Nil(err)

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

// Test Helpers
// SQS
func testSQSTargetAdapter(f func(c *target.SQSTargetConfig) (*target.SQSTargetConfig, error)) target.SQSTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.SQSTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected SQSTargetConfig")
		}

		return f(cfg)
	}

}

func testSQSTargetFunc(c *target.SQSTargetConfig) (*target.SQSTargetConfig, error) {

	return c, nil
}

// EventHub
func testEventHubTargetAdapter(f func(c *target.EventHubConfig) (*target.EventHubConfig, error)) target.EventHubTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.EventHubConfig)
		if !ok {
			return nil, errors.New("invalid input, expected EventHubTargetConfig")
		}

		return f(cfg)
	}

}

func testEventHubTargetFunc(c *target.EventHubConfig) (*target.EventHubConfig, error) {

	return c, nil
}

// HTTP
func testHTTPTargetAdapter(f func(c *target.HTTPTargetConfig) (*target.HTTPTargetConfig, error)) target.HTTPTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.HTTPTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected HTTPTargetConfig")
		}

		return f(cfg)
	}

}

func testHTTPTargetFunc(c *target.HTTPTargetConfig) (*target.HTTPTargetConfig, error) {

	return c, nil
}

// Kafka
func testKafkaTargetAdapter(f func(c *target.KafkaConfig) (*target.KafkaConfig, error)) target.KafkaTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.KafkaConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KafkaTargetConfig")
		}

		return f(cfg)
	}

}

func testKafkaTargetFunc(c *target.KafkaConfig) (*target.KafkaConfig, error) {

	return c, nil
}

// Kinesis
func testKinesisTargetAdapter(f func(c *target.KinesisTargetConfig) (*target.KinesisTargetConfig, error)) target.KinesisTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.KinesisTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KinesisTargetConfig")
		}

		return f(cfg)
	}

}

func testKinesisTargetFunc(c *target.KinesisTargetConfig) (*target.KinesisTargetConfig, error) {

	return c, nil
}

// PubSub
func testPubSubTargetAdapter(f func(c *target.PubSubTargetConfig) (*target.PubSubTargetConfig, error)) target.PubSubTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.PubSubTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected PubSubTargetConfig")
		}

		return f(cfg)
	}

}

func testPubSubTargetFunc(c *target.PubSubTargetConfig) (*target.PubSubTargetConfig, error) {

	return c, nil
}

// StatsD
func testStatsDAdapter(f func(c *statsreceiver.StatsDStatsReceiverConfig) (*statsreceiver.StatsDStatsReceiverConfig, error)) statsreceiver.StatsDStatsReceiverAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*statsreceiver.StatsDStatsReceiverConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StatsDStatsReceiverConfig")
		}

		return f(cfg)
	}

}

func testStatsDFunc(c *statsreceiver.StatsDStatsReceiverConfig) (*statsreceiver.StatsDStatsReceiverConfig, error) {

	return c, nil
}
