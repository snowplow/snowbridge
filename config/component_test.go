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
	"errors"
	"fmt"
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
			File: "targets/sqs-minimal-example.hcl",
			Plug: testSQSTargetAdapter(testSQSTargetFunc),
			Expected: &target.SQSTargetConfig{
				QueueName: "mySqsQueue",
				Region:    "us-west-1",
			},
		},
		{
			File: "targets/eventhub-minimal-example.hcl",
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
			File: "targets/eventhub-full-example.hcl",
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
				SetEHPartitionKey:       true,
			},
		},
		{
			File: "targets/http-minimal-example.hcl",
			Plug: testHTTPTargetAdapter(testHTTPTargetFunc),
			Expected: &target.HTTPTargetConfig{
				HTTPURL:                 "https://acme.com/x",
				RequestMaxMessages:      20,
				RequestByteLimit:        1048576,
				MessageByteLimit:        1048576,
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
			File: "targets/http-full-example.hcl",
			Plug: testHTTPTargetAdapter(testHTTPTargetFunc),
			Expected: &target.HTTPTargetConfig{
				HTTPURL:                 "https://acme.com/x",
				RequestMaxMessages:      100,
				RequestByteLimit:        1000000,
				MessageByteLimit:        1000000,
				RequestTimeoutInSeconds: 2,
				ContentType:             "text/html",
				Headers:                 "{\"Accept-Language\":\"en-US\"}",
				BasicAuthUsername:       "myUsername",
				BasicAuthPassword:       "myAuthPassword",
				OAuth2ClientID:          "myClientID",
				OAuth2ClientSecret:      "myClientSecret",
				OAuth2RefreshToken:      "myRefreshToken",
				OAuth2TokenURL:          "https://my.auth.server/token",
				CertFile:                "myLocalhost.crt",
				KeyFile:                 "myLocalhost.key",
				CaFile:                  "myRootCA.crt",
				SkipVerifyTLS:           true,
				DynamicHeaders:          true,
				TemplateFile:            "myTemplate.file",
			},
		},
		{
			File: "targets/kafka-minimal-example.hcl",
			Plug: testKafkaTargetAdapter(testKafkaTargetFunc),
			Expected: &target.KafkaConfig{
				Brokers:        "my-kafka-connection-string",
				TopicName:      "snowplow-enriched-good",
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
			File: "targets/kafka-full-example.hcl",
			Plug: testKafkaTargetAdapter(testKafkaTargetFunc),
			Expected: &target.KafkaConfig{
				Brokers:        "my-kafka-connection-string",
				TopicName:      "snowplow-enriched-good",
				TargetVersion:  "2.7.0",
				MaxRetries:     11,
				ByteLimit:      1000000,
				Compress:       true,
				WaitForAll:     true,
				Idempotent:     true,
				EnableSASL:     true,
				SASLUsername:   "mySaslUsername",
				SASLPassword:   "mySASLPassword",
				SASLAlgorithm:  "sha256",
				CertFile:       "myLocalhost.crt",
				KeyFile:        "myLocalhost.key",
				CaFile:         "myRootCA.crt",
				SkipVerifyTLS:  true,
				ForceSync:      true,
				FlushFrequency: 2,
				FlushMessages:  2,
				FlushBytes:     2,
			},
		},
		{
			File: "targets/kinesis-minimal-example.hcl",
			Plug: testKinesisTargetAdapter(testKinesisTargetFunc),
			Expected: &target.KinesisTargetConfig{
				StreamName:         "my-stream",
				Region:             "us-west-1",
				RoleARN:            "",
				RequestMaxMessages: 500,
			},
		},
		{
			File: "targets/pubsub-minimal-example.hcl",
			Plug: testPubSubTargetAdapter(testPubSubTargetFunc),
			Expected: &target.PubSubTargetConfig{
				ProjectID: "acme-project",
				TopicName: "some-acme-topic",
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(assets.AssetsRootDir, "docs", "configuration", tt.File) // TODO: change this to use the docs ones and remove the duplication
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			// Set env vars referenced in configs
			t.Setenv("CLIENT_ID", "myClientID")
			t.Setenv("CLIENT_SECRET", "myClientSecret")
			t.Setenv("REFRESH_TOKEN", "myRefreshToken")
			t.Setenv("SASL_PASSWORD", "mySASLPassword")
			t.Setenv("MY_AUTH_PASSWORD", "myAuthPassword")

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
			if err != nil {
				fmt.Println("#####################################")
				fmt.Println(err.Error())
			}

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
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
