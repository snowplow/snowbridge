// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/caarlos0/env/v6"
	"github.com/pkg/errors"

	"github.com/snowplow-devops/stream-replicator/pkg/failure"
	"github.com/snowplow-devops/stream-replicator/pkg/failure/failureiface"
	"github.com/snowplow-devops/stream-replicator/pkg/observer"
	"github.com/snowplow-devops/stream-replicator/pkg/source"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver/statsreceiveriface"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
	"github.com/snowplow-devops/stream-replicator/pkg/target/targetiface"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// ---------- [ TARGETS ] ----------

// KinesisTargetConfig configures the destination for records consumed
type KinesisTargetConfig struct {
	StreamName string `env:"TARGET_KINESIS_STREAM_NAME"`
	Region     string `env:"TARGET_KINESIS_REGION"`
	RoleARN    string `env:"TARGET_KINESIS_ROLE_ARN"`
}

// PubSubTargetConfig configures the destination for records consumed
type PubSubTargetConfig struct {
	ProjectID string `env:"TARGET_PUBSUB_PROJECT_ID"`
	TopicName string `env:"TARGET_PUBSUB_TOPIC_NAME"`
}

// SQSTargetConfig configures the destination for records consumed
type SQSTargetConfig struct {
	QueueName string `env:"TARGET_SQS_QUEUE_NAME"`
	Region    string `env:"TARGET_SQS_REGION"`
	RoleARN   string `env:"TARGET_SQS_ROLE_ARN"`
}

// KafkaTargetConfig configures the destination for records consumed
type KafkaTargetConfig struct {
	Brokers           string `env:"TARGET_KAFKA_BROKERS"`                            // REQUIRED
	TopicName         string `env:"TARGET_KAFKA_TOPIC_NAME"`                         // REQUIRED
	TargetVersion     string `env:"TARGET_KAFKA_TARGET_VERSION"`                     // The Kafka version we should target e.g. 2.7.0 or 0.11.0.2
	MaxRetries        int    `env:"TARGET_KAFKA_MAX_RETRIES" envDefault:"10"`        // Max retries
	ByteLimit         int    `env:"TARGET_KAFKA_BYTE_LIMIT" envDefault:"1048576"`    // Kafka Default is 1MiB
	Compress          bool   `env:"TARGET_KAFKA_COMPRESS"`                           // Reduces Network usage & Increases latency by compressing data
	WaitForAll        bool   `env:"TARGET_KAFKA_WAIT_FOR_ALL"`                       // Sets RequireAcks = WaitForAll which waits for min.insync.replicas to Ack
	Idempotent        bool   `env:"TARGET_KAFKA_IDEMPOTENT"`                         // Exactly once writes - Also sets RequiredAcks = WaitForAll
	EnableSASL        bool   `env:"TARGET_KAFKA_ENABLE_SASL"`                        // Enables SASL Support
	SASLUsername      string `env:"TARGET_KAFKA_SASL_USERNAME"`                      // SASL auth
	SASLPassword      string `env:"TARGET_KAFKA_SASL_PASSWORD"`                      // SASL auth
	SASLAlgorithm     string `env:"TARGET_KAFKA_SASL_ALGORITHM" envDefault:"sha512"` // sha256 or sha512
	CertFile          string `env:"TARGET_KAFKA_TLS_CERT_FILE"`                      // The optional certificate file for client authentication
	KeyFile           string `env:"TARGET_KAFKA_TLS_KEY_FILE"`                       // The optional key file for client authentication
	CaFile            string `env:"TARGET_KAFKA_TLS_CA_FILE"`                        // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS     bool   `env:"TARGET_KAFKA_TLS_SKIP_VERIFY_TLS"`                // Optional skip verifying ssl certificates chain
	ForceSyncProducer bool   `env:"TARGET_KAFKA_FORCE_SYNC_PRODUCER"`                // Forces the use of the Sync Producer, emits as fast as possible, may limit performance
	FlushFrequency    int    `env:"TARGET_KAFKA_FLUSH_FREQUENCY" envDefault:"0"`     // Milliseconds between flushes of events - 0 = as fast as possible
	FlushMessages     int    `env:"TARGET_KAFKA_FLUSH_MESSAGES" envDefault:"0"`      // Best effort for how many messages are sent in each batch - 0 = as fast as possible
	FlushBytes        int    `env:"TARGET_KAFKA_FLUSH_BYTES" envDefault:"0"`         // Best effort for how many bytes will trigger a flush - 0 = as fast as possible
}

// EventHubTargetConfig configures the destination for records consumed
type EventHubTargetConfig struct {
	EventHubNamespace       string `env:"TARGET_EVENTHUB_NAMESPACE"`                               // REQUIRED - namespace housing Eventhub
	EventHubName            string `env:"TARGET_EVENTHUB_NAME"`                                    // REQUIRED - name of Eventhub
	MaxAutoRetries          int    `env:"TARGET_EVENTHUB_MAX_AUTO_RETRY" envDefault:"1"`           // Number of retries handled automatically by the EH library - all retries should be completed before context timeout
	MessageByteLimit        int    `env:"TARGET_EVENTHUB_MESSAGE_BYTE_LIMIT" envDefault:"1048576"` // Default presumes paid tier limit is 1MB
	ChunkByteLimit          int    `env:"TARGET_EVENTHUB_CHUNK_BYTE_LIMIT" envDefault:"1048576"`   // Default chunk size of 1MB is arbitrary
	ChunkMessageLimit       int    `env:"TARGET_EVENTHUB_CHUNK_MESSAGE_LIMIT" envDefault:"500"`    // Default of 500 is arbitrary
	ContextTimeoutInSeconds int    `env:"TARGET_EVENTHUB_CONTEXT_TIMEOUT_SECONDS" envDefault:"20"` // Default of 20 is arbitrary
	BatchByteLimit          int    `env:"TARGET_EVENTHUB_BATCH_BYTE_LIMIT" envDefault:"1048576"`   // Default batch size of 1MB is the limit for EH's high tier
}

// HttpTargetConfig configures the destination for records consumed
type HttpTargetConfig struct {
	HttpUrl                 string `env:"TARGET_HTTP_URL"`                                        // REQUIRED - url endpoint
	ByteLimit               int    `env:"TARGET_HTTP_BYTE_LIMIT" envDefault:"1048576"`            // Byte limit for requests
	RequestTimeoutInSeconds int    `env:"TARGET_HTTP_TIMEOUT_IN_SECONDS" envDefault:"5"`          // Request timeout in seconds
	ContentType             string `env:"TARGET_HTTP_CONTENT_TYPE" envDefault:"application/json"` // Content type for POST request
	Headers                 string `env:"TARGET_HTTP_HEADERS"`                                    // Optional double-comma separated list of headers, eg: `{name1}::{value1},,{name2}::{value2}`
	BasicAuthUsername       string `env:"TARGET_HTTP_BASICAUTH_USERNAME"`                         // Optional basicauth username
	BasicAuthPassword       string `env:"TARGET_HTTP_BASICAUTH_PASSWORD"`                         // Optional basicauth password
	CertFile                string `env:"TARGET_HTTP_TLS_CERT_FILE"`                              // The optional certificate file for client authentication
	KeyFile                 string `env:"TARGET_HTTP_TLS_KEY_FILE"`                               // The optional key file for client authentication
	CaFile                  string `env:"TARGET_HTTP_TLS_CA_FILE"`                                // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS           bool   `env:"TARGET_HTTP_TLS_SKIP_VERIFY_TLS" envDefault:"false"`     // Optional skip verifying ssl certificates chain - if certfile and keyfile are not provided, this setting is not applied.
}

// TargetsConfig holds configuration for the available targets
type TargetsConfig struct {
	Kinesis  KinesisTargetConfig
	PubSub   PubSubTargetConfig
	SQS      SQSTargetConfig
	Kafka    KafkaTargetConfig
	EventHub EventHubTargetConfig
	Http     HttpTargetConfig
}

// ---------- [ FAILURE MESSAGE TARGETS ] ----------

// FailureKinesisTargetConfig configures the destination for records consumed
type FailureKinesisTargetConfig struct {
	StreamName string `env:"FAILURE_TARGET_KINESIS_STREAM_NAME"`
	Region     string `env:"FAILURE_TARGET_KINESIS_REGION"`
	RoleARN    string `env:"FAILURE_TARGET_KINESIS_ROLE_ARN"`
}

// FailurePubSubTargetConfig configures the destination for records consumed
type FailurePubSubTargetConfig struct {
	ProjectID string `env:"FAILURE_TARGET_PUBSUB_PROJECT_ID"`
	TopicName string `env:"FAILURE_TARGET_PUBSUB_TOPIC_NAME"`
}

// FailureSQSTargetConfig configures the destination for records consumed
type FailureSQSTargetConfig struct {
	QueueName string `env:"FAILURE_TARGET_SQS_QUEUE_NAME"`
	Region    string `env:"FAILURE_TARGET_SQS_REGION"`
	RoleARN   string `env:"FAILURE_TARGET_SQS_ROLE_ARN"`
}

// FailureKafkaTargetConfig configures the destination for records consumed
type FailureKafkaTargetConfig struct {
	Brokers           string `env:"FAILURE_TARGET_KAFKA_BROKERS"`                            // REQUIRED
	TopicName         string `env:"FAILURE_TARGET_KAFKA_TOPIC_NAME"`                         // REQUIRED
	TargetVersion     string `env:"FAILURE_TARGET_KAFKA_TARGET_VERSION"`                     // The Kafka version we should target e.g. 2.7.0 or 0.11.0.2
	MaxRetries        int    `env:"FAILURE_TARGET_KAFKA_MAX_RETRIES" envDefault:"10"`        // Max retries
	ByteLimit         int    `env:"FAILURE_TARGET_KAFKA_BYTE_LIMIT" envDefault:"1048576"`    // Kafka Default is 1MiB
	Compress          bool   `env:"FAILURE_TARGET_KAFKA_COMPRESS"`                           // Reduces Network usage & Increases latency by compressing data
	WaitForAll        bool   `env:"FAILURE_TARGET_KAFKA_WAIT_FOR_ALL"`                       // Sets RequireAcks = WaitForAll which waits for min.insync.replicas to Ack
	Idempotent        bool   `env:"FAILURE_TARGET_KAFKA_IDEMPOTENT"`                         // Exactly once writes
	EnableSASL        bool   `env:"FAILURE_TARGET_KAFKA_ENABLE_SASL"`                        // Enables SASL Support
	SASLUsername      string `env:"FAILURE_TARGET_KAFKA_SASL_USERNAME"`                      // SASL auth
	SASLPassword      string `env:"FAILURE_TARGET_KAFKA_SASL_PASSWORD"`                      // SASL auth
	SASLAlgorithm     string `env:"FAILURE_TARGET_KAFKA_SASL_ALGORITHM" envDefault:"sha512"` // sha256 or sha512
	CertFile          string `env:"FAILURE_TARGET_KAFKA_TLS_CERT_FILE"`                      // The optional certificate file for client authentication
	KeyFile           string `env:"FAILURE_TARGET_KAFKA_TLS_KEY_FILE"`                       // The optional key file for client authentication
	CaFile            string `env:"FAILURE_TARGET_KAFKA_TLS_CA_FILE"`                        // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS     bool   `env:"FAILURE_TARGET_KAFKA_TLS_SKIP_VERIFY_TLS"`                // Optional skip verifying ssl certificates chain
	ForceSyncProducer bool   `env:"FAILURE_TARGET_KAFKA_FORCE_SYNC_PRODUCER"`                // Forces the use of the Sync Producer, emits as fast as possible, may limit performance
	FlushFrequency    int    `env:"FAILURE_TARGET_KAFKA_FLUSH_FREQUENCY" envDefault:"0"`     // Milliseconds between flushes of events - 0 = as fast as possible
	FlushMessages     int    `env:"FAILURE_TARGET_KAFKA_FLUSH_MESSAGES" envDefault:"0"`      // Best effort for how many messages are sent in each batch - 0 = as fast as possible
	FlushBytes        int    `env:"FAILURE_TARGET_KAFKA_FLUSH_BYTES" envDefault:"0"`         // Best effort for how many bytes will trigger a flush - 0 = as fast as possible
}

// FailureEventHubTargetConfig configures the destination for records consumed
type FailureEventHubTargetConfig struct {
	EventHubNamespace       string `env:"FAILURE_TARGET_EVENTHUB_NAMESPACE"`                               // REQUIRED - namespace housing Eventhub
	EventHubName            string `env:"FAILURE_TARGET_EVENTHUB_NAME"`                                    // REQUIRED - name of Eventhub
	MaxAutoRetries          int    `env:"FAILURE_TARGET_EVENTHUB_MAX_AUTO_RETRY" envDefault:"1"`           // Number of retries handled automatically by the EH library - all retries should be completed before context timeout
	MessageByteLimit        int    `env:"FAILURE_TARGET_EVENTHUB_MESSAGE_BYTE_LIMIT" envDefault:"1048576"` // Default presumes paid tier limit is 1MB
	ChunkByteLimit          int    `env:"FAILURE_TARGET_EVENTHUB_CHUNK_BYTE_LIMIT" envDefault:"1048576"`   // Default chunk size of 1MB is arbitrary
	ChunkMessageLimit       int    `env:"FAILURE_TARGET_EVENTHUB_CHUNK_MESSAGE_LIMIT" envDefault:"500"`    // Default of 500 is arbitrary
	ContextTimeoutInSeconds int    `env:"FAILURE_TARGET_EVENTHUB_CONTEXT_TIMEOUT_SECONDS" envDefault:"20"` // Default of 20 is arbitrary
	BatchByteLimit          int    `env:"FAILURE_TARGET_EVENTHUB_BATCH_BYTE_LIMIT" envDefault:"1048576"`   // Default batch size of 1MB is the limit for EH's high tier
}

// FailureHttpTargetConfig configures the destination for records consumed
type FailureHttpTargetConfig struct {
	HttpUrl                 string `env:"FAILURE_TARGET_HTTP_URL"`                                        // REQUIRED - url endpoint
	byteLimit               int    `env:"FAILURE_TARGET_HTTP_BYTE_LIMIT" envDefault:"1048576"`            // Byte limit for requests
	requestTimeoutInSeconds int    `env:"FAILURE_TARGET_HTTP_TIMEOUT_IN_SECONDS" envDefault:"5"`          // Request timeout in seconds
	ContentType             string `env:"FAILURE_TARGET_HTTP_CONTENT_TYPE" envDefault:"application/json"` // Content type for POST request
	Headers                 string `env:"FAILURE_TARGET_HTTP_HEADERS"`                                    // Optional double-comma separated list of headers, eg: `{name1}::{value1},,{name2}::{value2}`
	BasicAuthUsername       string `env:"FAILURE_TARGET_HTTP_BASICAUTH_USERNAME"`                         // Optional basicauth username
	BasicAuthPassword       string `env:"FAILURE_TARGET_HTTP_BASICAUTH_PASSWORD"`                         // Optional basicauth password
	CertFile                string `env:"FAILURE_TARGET_HTTP_TLS_CERT_FILE"`                              // The optional certificate file for client authentication
	KeyFile                 string `env:"FAILURE_TARGET_HTTP_TLS_KEY_FILE"`                               // The optional key file for client authentication
	CaFile                  string `env:"FAILURE_TARGET_HTTP_TLS_CA_FILE"`                                // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS           bool   `env:"FAILURE_TARGET_HTTP_TLS_SKIP_VERIFY_TLS" envDefault:"false"`     // Optional skip verifying ssl certificates chain - if certfile and keyfile are not provided, this setting is not applied.
}

// FailureTargetsConfig holds configuration for the available targets
type FailureTargetsConfig struct {
	Kinesis  FailureKinesisTargetConfig
	PubSub   FailurePubSubTargetConfig
	SQS      FailureSQSTargetConfig
	Kafka    FailureKafkaTargetConfig
	EventHub FailureEventHubTargetConfig
	Http     FailureHttpTargetConfig

	// Format defines how the message will be transformed before
	// being sent to the target
	Format string `env:"FAILURE_TARGETS_FORMAT" envDefault:"snowplow"`
}

// ---------- [ SOURCES ] ----------

// KinesisSourceConfig configures the source for records pulled
type KinesisSourceConfig struct {
	StreamName     string `env:"SOURCE_KINESIS_STREAM_NAME"`
	Region         string `env:"SOURCE_KINESIS_REGION"`
	RoleARN        string `env:"SOURCE_KINESIS_ROLE_ARN"`
	AppName        string `env:"SOURCE_KINESIS_APP_NAME"`
	StartTimestamp string `env:"SOURCE_KINESIS_START_TIMESTAMP"` // Timestamp for the kinesis shard iterator to begin processing. Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
}

// PubSubSourceConfig configures the source for records pulled
type PubSubSourceConfig struct {
	ProjectID      string `env:"SOURCE_PUBSUB_PROJECT_ID"`
	SubscriptionID string `env:"SOURCE_PUBSUB_SUBSCRIPTION_ID"`
}

// SQSSourceConfig configures the source for records pulled
type SQSSourceConfig struct {
	QueueName string `env:"SOURCE_SQS_QUEUE_NAME"`
	Region    string `env:"SOURCE_SQS_REGION"`
	RoleARN   string `env:"SOURCE_SQS_ROLE_ARN"`
}

// SourcesConfig holds configuration for the available sources
type SourcesConfig struct {
	Kinesis KinesisSourceConfig
	PubSub  PubSubSourceConfig
	SQS     SQSSourceConfig

	// ConcurrentWrites is how many go-routines a source can leverage to parallelise processing
	ConcurrentWrites int `env:"SOURCE_CONCURRENT_WRITES" envDefault:"50"`
}

// ---------- [ OBSERVABILITY ] ----------

// SentryConfig configures the Sentry error tracker
type SentryConfig struct {
	Dsn   string `env:"SENTRY_DSN"`
	Tags  string `env:"SENTRY_TAGS" envDefault:"{}"`
	Debug bool   `env:"SENTRY_DEBUG" envDefault:"false"`
}

// StatsDStatsReceiverConfig configures the stats metrics receiver
type StatsDStatsReceiverConfig struct {
	Address string `env:"STATS_RECEIVER_STATSD_ADDRESS"`
	Prefix  string `env:"STATS_RECEIVER_STATSD_PREFIX" envDefault:"snowplow.stream-replicator"`
	Tags    string `env:"STATS_RECEIVER_STATSD_TAGS" envDefault:"{}"`
}

// StatsReceiversConfig holds configuration for different stats receivers
type StatsReceiversConfig struct {
	StatsD StatsDStatsReceiverConfig

	// TimeoutSec is how long the observer will wait for a new result before looping
	TimeoutSec int `env:"STATS_RECEIVER_TIMEOUT_SEC" envDefault:"1"`

	// BufferSec is how long the observer buffers results before pushing results out and resetting
	BufferSec int `env:"STATS_RECEIVER_BUFFER_SEC" envDefault:"15"`
}

// Config for holding all configuration details
type Config struct {
	Source         string `env:"SOURCE" envDefault:"stdin"`
	Sources        SourcesConfig
	Target         string `env:"TARGET" envDefault:"stdout"`
	Targets        TargetsConfig
	FailureTarget  string `env:"FAILURE_TARGET" envDefault:"stdout"`
	FailureTargets FailureTargetsConfig
	Transformation string `env:"MESSAGE_TRANSFORMATION" envDefault:"none"`
	LogLevel       string `env:"LOG_LEVEL" envDefault:"info"`
	Sentry         SentryConfig
	StatsReceiver  string `env:"STATS_RECEIVER"`
	StatsReceivers StatsReceiversConfig

	// Provides the ability to provide a GCP service account to the application directly
	GoogleServiceAccountB64 string `env:"GOOGLE_APPLICATION_CREDENTIALS_B64"`
}

// NewConfig resolves the config from the environment
func NewConfig() (*Config, error) {
	cfg := Config{}
	err := env.Parse(&cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// GetSource builds and returns the source that is configured
func (c *Config) GetSource() (sourceiface.Source, error) {
	switch c.Source {
	case "stdin":
		return source.NewStdinSource(
			c.Sources.ConcurrentWrites,
		)
	case "kinesis":
		var iteratorTstamp time.Time
		var tstampParseErr error
		if c.Sources.Kinesis.StartTimestamp != "" {
			iteratorTstamp, tstampParseErr = time.Parse("2006-01-02 15:04:05.999", c.Sources.Kinesis.StartTimestamp)
			if tstampParseErr != nil {
				return nil, errors.Wrap(tstampParseErr, fmt.Sprintf("Failed to parse provided value for SOURCE_KINESIS_START_TIMESTAMP: %v", iteratorTstamp))
			}
		}
		return source.NewKinesisSource(
			c.Sources.ConcurrentWrites,
			c.Sources.Kinesis.Region,
			c.Sources.Kinesis.StreamName,
			c.Sources.Kinesis.RoleARN,
			c.Sources.Kinesis.AppName,
			&iteratorTstamp,
		)
	case "pubsub":
		return source.NewPubSubSource(
			c.Sources.ConcurrentWrites,
			c.Sources.PubSub.ProjectID,
			c.Sources.PubSub.SubscriptionID,
		)
	case "sqs":
		return source.NewSQSSource(
			c.Sources.ConcurrentWrites,
			c.Sources.SQS.Region,
			c.Sources.SQS.QueueName,
			c.Sources.SQS.RoleARN,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid source found; expected one of 'stdin, kinesis, pubsub, sqs' and got '%s'", c.Source))
	}
}

// GetTarget builds and returns the target that is configured
func (c *Config) GetTarget() (targetiface.Target, error) {
	switch c.Target {
	case "stdout":
		return target.NewStdoutTarget()
	case "kinesis":
		return target.NewKinesisTarget(
			c.Targets.Kinesis.Region,
			c.Targets.Kinesis.StreamName,
			c.Targets.Kinesis.RoleARN,
		)
	case "pubsub":
		return target.NewPubSubTarget(
			c.Targets.PubSub.ProjectID,
			c.Targets.PubSub.TopicName,
		)
	case "sqs":
		return target.NewSQSTarget(
			c.Targets.SQS.Region,
			c.Targets.SQS.QueueName,
			c.Targets.SQS.RoleARN,
		)
	case "kafka":
		return target.NewKafkaTarget(&target.KafkaConfig{
			Brokers:        c.Targets.Kafka.Brokers,
			TopicName:      c.Targets.Kafka.TopicName,
			TargetVersion:  c.Targets.Kafka.TargetVersion,
			MaxRetries:     c.Targets.Kafka.MaxRetries,
			ByteLimit:      c.Targets.Kafka.ByteLimit,
			Compress:       c.Targets.Kafka.Compress,
			WaitForAll:     c.Targets.Kafka.WaitForAll,
			Idempotent:     c.Targets.Kafka.Idempotent,
			EnableSASL:     c.Targets.Kafka.EnableSASL,
			SASLUsername:   c.Targets.Kafka.SASLUsername,
			SASLPassword:   c.Targets.Kafka.SASLPassword,
			SASLAlgorithm:  c.Targets.Kafka.SASLAlgorithm,
			CertFile:       c.Targets.Kafka.CertFile,
			KeyFile:        c.Targets.Kafka.KeyFile,
			CaFile:         c.Targets.Kafka.CaFile,
			SkipVerifyTLS:  c.Targets.Kafka.SkipVerifyTLS,
			ForceSync:      c.Targets.Kafka.ForceSyncProducer,
			FlushFrequency: c.Targets.Kafka.FlushFrequency,
			FlushMessages:  c.Targets.Kafka.FlushMessages,
			FlushBytes:     c.Targets.Kafka.FlushBytes,
		})
	case "eventhub":
		return target.NewEventHubTarget(&target.EventHubConfig{
			EventHubNamespace:       c.Targets.EventHub.EventHubNamespace,
			EventHubName:            c.Targets.EventHub.EventHubName,
			MaxAutoRetries:          c.Targets.EventHub.MaxAutoRetries,
			MessageByteLimit:        c.Targets.EventHub.MessageByteLimit,
			ChunkByteLimit:          c.Targets.EventHub.ChunkByteLimit,
			ChunkMessageLimit:       c.Targets.EventHub.ChunkMessageLimit,
			ContextTimeoutInSeconds: c.Targets.EventHub.ContextTimeoutInSeconds,
			BatchByteLimit:          c.Targets.EventHub.BatchByteLimit,
		})
	case "http":
		return target.NewHttpTarget(
			c.Targets.Http.HttpUrl,
			c.Targets.Http.RequestTimeoutInSeconds,
			c.Targets.Http.ByteLimit,
			c.Targets.Http.ContentType,
			c.Targets.Http.Headers,
			c.Targets.Http.BasicAuthUsername,
			c.Targets.Http.BasicAuthPassword,
			c.Targets.Http.CertFile,
			c.Targets.Http.KeyFile,
			c.Targets.Http.CaFile,
			c.Targets.Http.SkipVerifyTLS,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got '%s'", c.Target))
	}
}

// GetFailureTarget builds and returns the target that is configured
func (c *Config) GetFailureTarget() (failureiface.Failure, error) {
	var t targetiface.Target
	var err error

	switch c.FailureTarget {
	case "stdout":
		t, err = target.NewStdoutTarget()
	case "kinesis":
		t, err = target.NewKinesisTarget(
			c.FailureTargets.Kinesis.Region,
			c.FailureTargets.Kinesis.StreamName,
			c.FailureTargets.Kinesis.RoleARN,
		)
	case "pubsub":
		t, err = target.NewPubSubTarget(
			c.FailureTargets.PubSub.ProjectID,
			c.FailureTargets.PubSub.TopicName,
		)
	case "sqs":
		t, err = target.NewSQSTarget(
			c.FailureTargets.SQS.Region,
			c.FailureTargets.SQS.QueueName,
			c.FailureTargets.SQS.RoleARN,
		)
	case "kafka":
		t, err = target.NewKafkaTarget(&target.KafkaConfig{
			Brokers:        c.FailureTargets.Kafka.Brokers,
			TopicName:      c.FailureTargets.Kafka.TopicName,
			TargetVersion:  c.FailureTargets.Kafka.TargetVersion,
			MaxRetries:     c.FailureTargets.Kafka.MaxRetries,
			ByteLimit:      c.FailureTargets.Kafka.ByteLimit,
			Compress:       c.FailureTargets.Kafka.Compress,
			WaitForAll:     c.FailureTargets.Kafka.WaitForAll,
			Idempotent:     c.FailureTargets.Kafka.Idempotent,
			EnableSASL:     c.FailureTargets.Kafka.EnableSASL,
			SASLUsername:   c.FailureTargets.Kafka.SASLUsername,
			SASLPassword:   c.FailureTargets.Kafka.SASLPassword,
			SASLAlgorithm:  c.FailureTargets.Kafka.SASLAlgorithm,
			CertFile:       c.FailureTargets.Kafka.CertFile,
			KeyFile:        c.FailureTargets.Kafka.KeyFile,
			CaFile:         c.FailureTargets.Kafka.CaFile,
			SkipVerifyTLS:  c.FailureTargets.Kafka.SkipVerifyTLS,
			ForceSync:      c.FailureTargets.Kafka.ForceSyncProducer,
			FlushFrequency: c.FailureTargets.Kafka.FlushFrequency,
			FlushMessages:  c.FailureTargets.Kafka.FlushMessages,
			FlushBytes:     c.FailureTargets.Kafka.FlushBytes,
		})
	case "eventhub":
		t, err = target.NewEventHubTarget(&target.EventHubConfig{
			EventHubNamespace:       c.FailureTargets.EventHub.EventHubNamespace,
			EventHubName:            c.FailureTargets.EventHub.EventHubName,
			MaxAutoRetries:          c.FailureTargets.EventHub.MaxAutoRetries,
			MessageByteLimit:        c.FailureTargets.EventHub.MessageByteLimit,
			ChunkByteLimit:          c.FailureTargets.EventHub.ChunkByteLimit,
			ChunkMessageLimit:       c.FailureTargets.EventHub.ChunkMessageLimit,
			ContextTimeoutInSeconds: c.FailureTargets.EventHub.ContextTimeoutInSeconds,
			BatchByteLimit:          c.FailureTargets.EventHub.BatchByteLimit,
		})
	case "http":
		t, err = target.NewHttpTarget(
			c.FailureTargets.Http.HttpUrl,
			c.FailureTargets.Http.requestTimeoutInSeconds,
			c.FailureTargets.Http.byteLimit,
			c.FailureTargets.Http.ContentType,
			c.FailureTargets.Http.Headers,
			c.FailureTargets.Http.BasicAuthUsername,
			c.FailureTargets.Http.BasicAuthPassword,
			c.FailureTargets.Http.CertFile,
			c.FailureTargets.Http.KeyFile,
			c.FailureTargets.Http.CaFile,
			c.FailureTargets.Http.SkipVerifyTLS,
		)
	default:
		err = errors.New(fmt.Sprintf("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got '%s'", c.FailureTarget))
	}
	if err != nil {
		return nil, err
	}

	switch c.FailureTargets.Format {
	case "snowplow":
		return failure.NewSnowplowFailure(t, AppName, AppVersion)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid failure format found; expected one of 'snowplow' and got '%s'", c.FailureTargets.Format))
	}
}

// GetTransformations builds and returns transformationApplyFunction from the transformations configured
func (c *Config) GetTransformations() (transform.TransformationApplyFunction, error) {
	funcs := make([]transform.TransformationFunction, 0, 0)

	// Parse list of transformations
	transformations := strings.Split(c.Transformation, ",")

	for _, transformation := range transformations {
		// Parse function name-option sets
		funcOpts := strings.Split(transformation, ":")

		switch funcOpts[0] {
		case "spEnrichedToJson":
			funcs = append(funcs, transform.SpEnrichedToJSON)
		case "spEnrichedSetPk":
			funcs = append(funcs, transform.NewSpEnrichedSetPkFunction(funcOpts[1]))
		case "spEnrichedFilter":
			filterFunc, err := transform.NewSpEnrichedFilterFunction(funcOpts[1])
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "none":
		default:
			return nil, errors.New(fmt.Sprintf("Invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk:{option}', spEnrichedFilter:{option} and got '%s'", c.Transformation))
		}
	}
	return transform.NewTransformation(funcs...), nil
}

// GetTags returns a list of tags to use in identifying this instance of stream-replicator
func (c *Config) GetTags(sourceID string, targetID string, failureTargetID string) (map[string]string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get server hostname as tag")
	}

	processID := os.Getpid()

	tags := map[string]string{
		"hostname":          hostname,
		"process_id":        strconv.Itoa(processID),
		"source_id":         sourceID,
		"target_id":         targetID,
		"failure_target_id": failureTargetID,
	}

	return tags, nil
}

// GetObserver builds and returns the observer with the embedded
// optional stats receiver
func (c *Config) GetObserver(tags map[string]string) (*observer.Observer, error) {
	sr, err := c.GetStatsReceiver(tags)
	if err != nil {
		return nil, err
	}
	return observer.New(sr, time.Duration(c.StatsReceivers.TimeoutSec)*time.Second, time.Duration(c.StatsReceivers.BufferSec)*time.Second), nil
}

// GetStatsReceiver builds and returns the stats receiver
func (c *Config) GetStatsReceiver(tags map[string]string) (statsreceiveriface.StatsReceiver, error) {
	switch c.StatsReceiver {
	case "statsd":
		return statsreceiver.NewStatsDStatsReceiver(
			c.StatsReceivers.StatsD.Address,
			c.StatsReceivers.StatsD.Prefix,
			c.StatsReceivers.StatsD.Tags,
			tags,
		)
	case "":
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid stats receiver found; expected one of 'statsd' and got '%s'", c.StatsReceiver))
	}
}
