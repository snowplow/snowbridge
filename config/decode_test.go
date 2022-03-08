// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
)

func TestHoconDecode(t *testing.T) {
	testCases := []struct {
		File     string
		Target   Config
		Expected Config
	}{
		{
			"empty.hocon",
			defaultConfigValue(),
			defaultConfigValue(),
		},

		{
			"defaults.hocon",
			emptyConfigValue(),
			Config{
				Source: "stdin",
				Sources: SourcesConfig{
					ConcurrentWrites: 50,
				},
				Target:        "stdout",
				FailureTarget: "stdout",
				FailureTargets: FailureTargetsConfig{
					Format: "snowplow",
				},
				Transformation: "none",
				LogLevel:       "info",
			},
		},

		{
			"example.hocon",
			defaultConfigValue(),
			expectedExample(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join("test-fixtures", tt.File)
			src, err := os.ReadFile(filename)
			assert.Nil(err)

			err = HoconDecode(src, &tt.Target)
			assert.Nil(err)

			if !reflect.DeepEqual(tt.Target, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(tt.Target),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestNewConfigFromFile(t *testing.T) {
	testCases := []struct {
		File     string
		Expected Config
	}{
		{
			"test-fixtures/example.hocon",
			expectedExample(),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", tt.File)

			cfg, err := NewConfig()
			assert.Nil(err)

			if !reflect.DeepEqual(*cfg, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(*cfg),
					spew.Sdump(tt.Expected))
			}
		})

	}

}

func emptyConfigValue() Config {
	return Config{}
}

func defaultConfigValue() Config {
	var defBytes int = 1048576

	// Default options for Targets
	defKafkaTargetOpts := KafkaTargetConfig{
		MaxRetries:     10,
		ByteLimit:      defBytes,
		SASLAlgorithm:  "sha512",
		FlushFrequency: 0,
		FlushMessages:  0,
		FlushBytes:     0,
	}
	defEventHubTargetOpts := EventHubTargetConfig{
		MaxAutoRetries:          1,
		MessageByteLimit:        defBytes,
		ChunkByteLimit:          defBytes,
		ChunkMessageLimit:       500,
		ContextTimeoutInSeconds: 20,
		BatchByteLimit:          defBytes,
	}
	defHTTPTargetOpts := HTTPTargetConfig{
		ByteLimit:               defBytes,
		RequestTimeoutInSeconds: 5,
		ContentType:             "application/json",
		SkipVerifyTLS:           false,
	}
	defTargetsOpts := TargetsConfig{
		Kafka:    defKafkaTargetOpts,
		EventHub: defEventHubTargetOpts,
		HTTP:     defHTTPTargetOpts,
	}

	// Default options for Failure Targets
	defFailKafkaTargetOpts := FailureKafkaTargetConfig{
		MaxRetries:     10,
		ByteLimit:      defBytes,
		SASLAlgorithm:  "sha512",
		FlushFrequency: 0,
		FlushMessages:  0,
		FlushBytes:     0,
	}
	defFailEventHubTargetOpts := FailureEventHubTargetConfig{
		MaxAutoRetries:          1,
		MessageByteLimit:        defBytes,
		ChunkByteLimit:          defBytes,
		ChunkMessageLimit:       500,
		ContextTimeoutInSeconds: 20,
		BatchByteLimit:          defBytes,
	}
	defFailHTTPTargetOpts := FailureHTTPTargetConfig{
		byteLimit:               defBytes,
		requestTimeoutInSeconds: 5,
		ContentType:             "application/json",
		SkipVerifyTLS:           false,
	}
	defFailTargetsOpts := FailureTargetsConfig{
		Kafka:    defFailKafkaTargetOpts,
		EventHub: defFailEventHubTargetOpts,
		HTTP:     defFailHTTPTargetOpts,
		Format:   "snowplow",
	}

	// Default options for Sources
	defSourcesOpts := SourcesConfig{
		ConcurrentWrites: 50,
	}

	// Default observability options
	defSentryOpts := SentryConfig{
		Tags:  "{}",
		Debug: false,
	}
	defStatsDOpts := StatsDStatsReceiverConfig{
		Prefix: "snowplow.stream-replicator",
		Tags:   "{}",
	}
	defStatsReceiversOpts := StatsReceiversConfig{
		StatsD:     defStatsDOpts,
		TimeoutSec: 1,
		BufferSec:  15,
	}

	// Root Config default options
	defConfig := Config{
		Source:         "stdin",
		Sources:        defSourcesOpts,
		Target:         "stdout",
		Targets:        defTargetsOpts,
		FailureTarget:  "stdout",
		FailureTargets: defFailTargetsOpts,
		Transformation: "none",
		LogLevel:       "info",
		Sentry:         defSentryOpts,
		StatsReceivers: defStatsReceiversOpts,
	}

	return defConfig
}

func expectedExample() Config {
	c := Config{
		Source: "kinesis",
		Sources: SourcesConfig{
			Kinesis: KinesisSourceConfig{
				StreamName:     "test_stream_name",
				Region:         "test_region",
				RoleARN:        "test_role_arn",
				AppName:        "test_app_name",
				StartTimestamp: "2020-01-01 01:01:01",
			},
			ConcurrentWrites: 50,
		},
		Target: "pubsub",
		Targets: TargetsConfig{
			PubSub: PubSubTargetConfig{
				ProjectID: "test_project_id",
				TopicName: "test_topic_name",
			},
			Kafka: KafkaTargetConfig{
				MaxRetries:     10,
				ByteLimit:      1048576,
				SASLAlgorithm:  "sha512",
				FlushFrequency: 0,
				FlushMessages:  0,
				FlushBytes:     0,
			},
			EventHub: EventHubTargetConfig{
				MaxAutoRetries:          1,
				MessageByteLimit:        1048576,
				ChunkByteLimit:          1048576,
				ChunkMessageLimit:       500,
				ContextTimeoutInSeconds: 20,
				BatchByteLimit:          1048576,
			},
			HTTP: HTTPTargetConfig{
				ByteLimit:               1048576,
				RequestTimeoutInSeconds: 5,
				ContentType:             "application/json",
				SkipVerifyTLS:           false,
			},
		},
		FailureTarget: "kafka",
		FailureTargets: FailureTargetsConfig{
			Kafka: FailureKafkaTargetConfig{
				Brokers:           "test_brokers",
				TopicName:         "test_topic_name",
				TargetVersion:     "0.0.0",
				MaxRetries:        10,
				ByteLimit:         1048576,
				Compress:          true,
				WaitForAll:        false,
				Idempotent:        true,
				EnableSASL:        true,
				SASLUsername:      "test_username",
				SASLPassword:      "test_password",
				SASLAlgorithm:     "sha512",
				CertFile:          "x.x",
				KeyFile:           "y.y",
				CaFile:            "z.z",
				SkipVerifyTLS:     true,
				ForceSyncProducer: false,
				FlushFrequency:    1,
				FlushMessages:     2,
				FlushBytes:        3,
			},
			EventHub: FailureEventHubTargetConfig{
				MaxAutoRetries:          1,
				MessageByteLimit:        1048576,
				ChunkByteLimit:          1048576,
				ChunkMessageLimit:       500,
				ContextTimeoutInSeconds: 20,
				BatchByteLimit:          1048576,
			},
			HTTP: FailureHTTPTargetConfig{
				byteLimit:               1048576,
				requestTimeoutInSeconds: 5,
				ContentType:             "application/json",
				SkipVerifyTLS:           false,
			},
			Format: "snowplow",
		},
		Transformation: "none",
		LogLevel:       "info",
		Sentry: SentryConfig{
			Dsn:   "test_dsn",
			Tags:  "{\"key\":\"sentry\"}",
			Debug: true,
		},
		StatsReceiver: "statsd",
		StatsReceivers: StatsReceiversConfig{
			StatsD: StatsDStatsReceiverConfig{
				Address: "test_address",
				Prefix:  "test_prefix",
				Tags:    "{\"key\":\"statsd\"}",
			},
			TimeoutSec: 2,
			BufferSec:  20,
		},
		GoogleServiceAccountB64: "test_creds",
	}

	return c
}
