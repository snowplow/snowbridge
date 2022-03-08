// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/snowplow-devops/stream-replicator/pkg/failure"
	"github.com/snowplow-devops/stream-replicator/pkg/failure/failureiface"
	"github.com/snowplow-devops/stream-replicator/pkg/observer"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver/statsreceiveriface"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
	"github.com/snowplow-devops/stream-replicator/pkg/target/targetiface"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// ---------- [ TARGETS ] ----------

// KinesisTargetConfig configures the destination for records consumed
type KinesisTargetConfig struct {
	StreamName string `env:"TARGET_KINESIS_STREAM_NAME" json:"stream_name"`
	Region     string `env:"TARGET_KINESIS_REGION" json:"region"`
	RoleARN    string `env:"TARGET_KINESIS_ROLE_ARN" json:"role_arn"`
}

// PubSubTargetConfig configures the destination for records consumed
type PubSubTargetConfig struct {
	ProjectID string `env:"TARGET_PUBSUB_PROJECT_ID" json:"project_id"`
	TopicName string `env:"TARGET_PUBSUB_TOPIC_NAME" json:"topic_name"`
}

// SQSTargetConfig configures the destination for records consumed
type SQSTargetConfig struct {
	QueueName string `env:"TARGET_SQS_QUEUE_NAME" json:"queue_name"`
	Region    string `env:"TARGET_SQS_REGION" json:"region"`
	RoleARN   string `env:"TARGET_SQS_ROLE_ARN" json:"role_arn"`
}

// KafkaTargetConfig configures the destination for records consumed
type KafkaTargetConfig struct {
	Brokers           string `env:"TARGET_KAFKA_BROKERS" json:"brokers"`                                       // REQUIRED
	TopicName         string `env:"TARGET_KAFKA_TOPIC_NAME" json:"topic_name"`                                 // REQUIRED
	TargetVersion     string `env:"TARGET_KAFKA_TARGET_VERSION" json:"target_version"`                         // The Kafka version we should target e.g. 2.7.0 or 0.11.0.2
	MaxRetries        int    `env:"TARGET_KAFKA_MAX_RETRIES" envDefault:"10" json:"max_retries,string"`        // Max retries
	ByteLimit         int    `env:"TARGET_KAFKA_BYTE_LIMIT" envDefault:"1048576" json:"byte_limit,string"`     // Kafka Default is 1MiB
	Compress          bool   `env:"TARGET_KAFKA_COMPRESS" json:"compress,string"`                              // Reduces Network usage & Increases latency by compressing data
	WaitForAll        bool   `env:"TARGET_KAFKA_WAIT_FOR_ALL" json:"wait_for_all,string"`                      // Sets RequireAcks = WaitForAll which waits for min.insync.replicas to Ack
	Idempotent        bool   `env:"TARGET_KAFKA_IDEMPOTENT" json:"idempotent,string"`                          // Exactly once writes - Also sets RequiredAcks = WaitForAll
	EnableSASL        bool   `env:"TARGET_KAFKA_ENABLE_SASL" json:"enable_sasl,string"`                        // Enables SASL Support
	SASLUsername      string `env:"TARGET_KAFKA_SASL_USERNAME" json:"sasl_username"`                           // SASL auth
	SASLPassword      string `env:"TARGET_KAFKA_SASL_PASSWORD" json:"sasl_password"`                           // SASL auth
	SASLAlgorithm     string `env:"TARGET_KAFKA_SASL_ALGORITHM" envDefault:"sha512" json:"sasl_algorithm"`     // sha256 or sha512
	CertFile          string `env:"TARGET_KAFKA_TLS_CERT_FILE" json:"cert_file"`                               // The optional certificate file for client authentication
	KeyFile           string `env:"TARGET_KAFKA_TLS_KEY_FILE" json:"key_file"`                                 // The optional key file for client authentication
	CaFile            string `env:"TARGET_KAFKA_TLS_CA_FILE" json:"ca_file"`                                   // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS     bool   `env:"TARGET_KAFKA_TLS_SKIP_VERIFY_TLS" json:"skip_verify_tls,string"`            // Optional skip verifying ssl certificates chain
	ForceSyncProducer bool   `env:"TARGET_KAFKA_FORCE_SYNC_PRODUCER" json:"force_sync_producer,string"`        // Forces the use of the Sync Producer, emits as fast as possible, may limit performance
	FlushFrequency    int    `env:"TARGET_KAFKA_FLUSH_FREQUENCY" envDefault:"0" json:"flush_frequency,string"` // Milliseconds between flushes of events - 0 = as fast as possible
	FlushMessages     int    `env:"TARGET_KAFKA_FLUSH_MESSAGES" envDefault:"0" json:"flush_messages,string"`   // Best effort for how many messages are sent in each batch - 0 = as fast as possible
	FlushBytes        int    `env:"TARGET_KAFKA_FLUSH_BYTES" envDefault:"0" json:"flush_bytes,string"`         // Best effort for how many bytes will trigger a flush - 0 = as fast as possible
}

// EventHubTargetConfig configures the destination for records consumed
type EventHubTargetConfig struct {
	EventHubNamespace       string `env:"TARGET_EVENTHUB_NAMESPACE" json:"namespace"`                                                // REQUIRED - namespace housing Eventhub
	EventHubName            string `env:"TARGET_EVENTHUB_NAME" json:"name"`                                                          // REQUIRED - name of Eventhub
	MaxAutoRetries          int    `env:"TARGET_EVENTHUB_MAX_AUTO_RETRY" envDefault:"1" json:"max_auto_retries,string"`              // Number of retries handled automatically by the EH library - all retries should be completed before context timeout
	MessageByteLimit        int    `env:"TARGET_EVENTHUB_MESSAGE_BYTE_LIMIT" envDefault:"1048576" json:"message_byte_limit,string"`  // Default presumes paid tier limit is 1MB
	ChunkByteLimit          int    `env:"TARGET_EVENTHUB_CHUNK_BYTE_LIMIT" envDefault:"1048576" json:"chunk_byte_limit,string"`      // Default chunk size of 1MB is arbitrary
	ChunkMessageLimit       int    `env:"TARGET_EVENTHUB_CHUNK_MESSAGE_LIMIT" envDefault:"500" json:"chunk_message_limit,string"`    // Default of 500 is arbitrary
	ContextTimeoutInSeconds int    `env:"TARGET_EVENTHUB_CONTEXT_TIMEOUT_SECONDS" envDefault:"20" context_timeout_in_seconds,string` // Default of 20 is arbitrary
	BatchByteLimit          int    `env:"TARGET_EVENTHUB_BATCH_BYTE_LIMIT" envDefault:"1048576" json:"batch_byte_limit,string"`      // Default batch size of 1MB is the limit for EH's high tier
}

// HTTPTargetConfig configures the destination for records consumed
type HTTPTargetConfig struct {
	HTTPURL                 string `env:"TARGET_HTTP_URL" json:"url"`                                                             // REQUIRED - url endpoint
	ByteLimit               int    `env:"TARGET_HTTP_BYTE_LIMIT" envDefault:"1048576" json:"byte_limit,string"`                   // Byte limit for requests
	RequestTimeoutInSeconds int    `env:"TARGET_HTTP_TIMEOUT_IN_SECONDS" envDefault:"5" json:"request_timeout_in_seconds,string"` // Request timeout in seconds
	ContentType             string `env:"TARGET_HTTP_CONTENT_TYPE" envDefault:"application/json" json:"content_type"`             // Content type for POST request
	Headers                 string `env:"TARGET_HTTP_HEADERS" json:"headers"`                                                     // Optional headers to add to the request, provided as a JSON of string key-value pairs. eg: `{"Max Forwards": "10", "Accept-Language": "en-US,en-IE", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}`
	BasicAuthUsername       string `env:"TARGET_HTTP_BASICAUTH_USERNAME" json:"basic_auth_username"`                              // Optional basicauth username
	BasicAuthPassword       string `env:"TARGET_HTTP_BASICAUTH_PASSWORD" json:"basic_auth_password"`                              // Optional basicauth password
	CertFile                string `env:"TARGET_HTTP_TLS_CERT_FILE" json:"cert_file"`                                             // The optional certificate file for client authentication
	KeyFile                 string `env:"TARGET_HTTP_TLS_KEY_FILE" json:"key_file"`                                               // The optional key file for client authentication
	CaFile                  string `env:"TARGET_HTTP_TLS_CA_FILE" json:"ca_file"`                                                 // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS           bool   `env:"TARGET_HTTP_TLS_SKIP_VERIFY_TLS" envDefault:"false" json:"skip_verify_tls,string"`       // Optional skip verifying ssl certificates chain - if certfile and keyfile are not provided, this setting is not applied.
}

// TargetsConfig holds configuration for the available targets
type TargetsConfig struct {
	Kinesis  KinesisTargetConfig  `json:"kinesis"`
	PubSub   PubSubTargetConfig   `json:"pubsub"`
	SQS      SQSTargetConfig      `json:"sqs"`
	Kafka    KafkaTargetConfig    `json:"kafka"`
	EventHub EventHubTargetConfig `json:"eventhub"`
	HTTP     HTTPTargetConfig     `json:"http"`
}

// ---------- [ FAILURE MESSAGE TARGETS ] ----------

// FailureKinesisTargetConfig configures the destination for records consumed
type FailureKinesisTargetConfig struct {
	StreamName string `env:"FAILURE_TARGET_KINESIS_STREAM_NAME" json:"stream_name"`
	Region     string `env:"FAILURE_TARGET_KINESIS_REGION" json:"region"`
	RoleARN    string `env:"FAILURE_TARGET_KINESIS_ROLE_ARN" json:"role_arn"`
}

// FailurePubSubTargetConfig configures the destination for records consumed
type FailurePubSubTargetConfig struct {
	ProjectID string `env:"FAILURE_TARGET_PUBSUB_PROJECT_ID" json:"project_id"`
	TopicName string `env:"FAILURE_TARGET_PUBSUB_TOPIC_NAME" json:"topic_name"`
}

// FailureSQSTargetConfig configures the destination for records consumed
type FailureSQSTargetConfig struct {
	QueueName string `env:"FAILURE_TARGET_SQS_QUEUE_NAME" json:"queue_name"`
	Region    string `env:"FAILURE_TARGET_SQS_REGION" json:"region"`
	RoleARN   string `env:"FAILURE_TARGET_SQS_ROLE_ARN" json:"role_arn"`
}

// FailureKafkaTargetConfig configures the destination for records consumed
type FailureKafkaTargetConfig struct {
	Brokers           string `env:"FAILURE_TARGET_KAFKA_BROKERS" json:"brokers"`                                       // REQUIRED
	TopicName         string `env:"FAILURE_TARGET_KAFKA_TOPIC_NAME" json:"topic_name"`                                 // REQUIRED
	TargetVersion     string `env:"FAILURE_TARGET_KAFKA_TARGET_VERSION" json:"target_version"`                         // The Kafka version we should target e.g. 2.7.0 or 0.11.0.2
	MaxRetries        int    `env:"FAILURE_TARGET_KAFKA_MAX_RETRIES" envDefault:"10" json:"max_retries,string"`        // Max retries
	ByteLimit         int    `env:"FAILURE_TARGET_KAFKA_BYTE_LIMIT" envDefault:"1048576" json:"byte_limit,string"`     // Kafka Default is 1MiB
	Compress          bool   `env:"FAILURE_TARGET_KAFKA_COMPRESS" json:"compress,string"`                              // Reduces Network usage & Increases latency by compressing data
	WaitForAll        bool   `env:"FAILURE_TARGET_KAFKA_WAIT_FOR_ALL" json:"wait_for_all,string"`                      // Sets RequireAcks = WaitForAll which waits for min.insync.replicas to Ack
	Idempotent        bool   `env:"FAILURE_TARGET_KAFKA_IDEMPOTENT" json:"idempotent,string"`                          // Exactly once writes
	EnableSASL        bool   `env:"FAILURE_TARGET_KAFKA_ENABLE_SASL" json:"enable_sasl,string"`                        // Enables SASL Support
	SASLUsername      string `env:"FAILURE_TARGET_KAFKA_SASL_USERNAME" json:"sasl_username"`                           // SASL auth
	SASLPassword      string `env:"FAILURE_TARGET_KAFKA_SASL_PASSWORD" json:"sasl_password"`                           // SASL auth
	SASLAlgorithm     string `env:"FAILURE_TARGET_KAFKA_SASL_ALGORITHM" envDefault:"sha512" json:"sasl_algorithm"`     // sha256 or sha512
	CertFile          string `env:"FAILURE_TARGET_KAFKA_TLS_CERT_FILE" json:"cert_file"`                               // The optional certificate file for client authentication
	KeyFile           string `env:"FAILURE_TARGET_KAFKA_TLS_KEY_FILE" json:"key_file"`                                 // The optional key file for client authentication
	CaFile            string `env:"FAILURE_TARGET_KAFKA_TLS_CA_FILE" json:"ca_file"`                                   // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS     bool   `env:"FAILURE_TARGET_KAFKA_TLS_SKIP_VERIFY_TLS" json:"skip_verify_tls,string"`            // Optional skip verifying ssl certificates chain
	ForceSyncProducer bool   `env:"FAILURE_TARGET_KAFKA_FORCE_SYNC_PRODUCER" json:"force_sync_producer,string"`        // Forces the use of the Sync Producer, emits as fast as possible, may limit performance
	FlushFrequency    int    `env:"FAILURE_TARGET_KAFKA_FLUSH_FREQUENCY" envDefault:"0" json:"flush_frequency,string"` // Milliseconds between flushes of events - 0 = as fast as possible
	FlushMessages     int    `env:"FAILURE_TARGET_KAFKA_FLUSH_MESSAGES" envDefault:"0" json:"flush_messages,string"`   // Best effort for how many messages are sent in each batch - 0 = as fast as possible
	FlushBytes        int    `env:"FAILURE_TARGET_KAFKA_FLUSH_BYTES" envDefault:"0" json:"flush_bytes,string"`         // Best effort for how many bytes will trigger a flush - 0 = as fast as possible
}

// FailureEventHubTargetConfig configures the destination for records consumed
type FailureEventHubTargetConfig struct {
	EventHubNamespace       string `env:"FAILURE_TARGET_EVENTHUB_NAMESPACE" json:"namespace"`                                                       // REQUIRED - namespace housing Eventhub
	EventHubName            string `env:"FAILURE_TARGET_EVENTHUB_NAME" json:"name"`                                                                 // REQUIRED - name of Eventhub
	MaxAutoRetries          int    `env:"FAILURE_TARGET_EVENTHUB_MAX_AUTO_RETRY" envDefault:"1" json:"max_auto_retries,string"`                     // Number of retries handled automatically by the EH library - all retries should be completed before context timeout
	MessageByteLimit        int    `env:"FAILURE_TARGET_EVENTHUB_MESSAGE_BYTE_LIMIT" envDefault:"1048576" json:"message_byte_limit,string"`         // Default presumes paid tier limit is 1MB
	ChunkByteLimit          int    `env:"FAILURE_TARGET_EVENTHUB_CHUNK_BYTE_LIMIT" envDefault:"1048576" json:"chunk_byte_limit,string"`             // Default chunk size of 1MB is arbitrary
	ChunkMessageLimit       int    `env:"FAILURE_TARGET_EVENTHUB_CHUNK_MESSAGE_LIMIT" envDefault:"500" json:"chunk_message_limit,string"`           // Default of 500 is arbitrary
	ContextTimeoutInSeconds int    `env:"FAILURE_TARGET_EVENTHUB_CONTEXT_TIMEOUT_SECONDS" envDefault:"20" json:"context_timeout_in_seconds,string"` // Default of 20 is arbitrary
	BatchByteLimit          int    `env:"FAILURE_TARGET_EVENTHUB_BATCH_BYTE_LIMIT" envDefault:"1048576" json:"batch_byte_limit,string"`             // Default batch size of 1MB is the limit for EH's high tier
}

// FailureHTTPTargetConfig configures the destination for records consumed
type FailureHTTPTargetConfig struct {
	HTTPURL                 string `env:"FAILURE_TARGET_HTTP_URL" json:"url"`                                                             // REQUIRED - url endpoint
	byteLimit               int    `env:"FAILURE_TARGET_HTTP_BYTE_LIMIT" envDefault:"1048576" json:"byte_limit,string"`                   // Byte limit for requests
	requestTimeoutInSeconds int    `env:"FAILURE_TARGET_HTTP_TIMEOUT_IN_SECONDS" envDefault:"5" json:"request_timeout_in_seconds,string"` // Request timeout in seconds
	ContentType             string `env:"FAILURE_TARGET_HTTP_CONTENT_TYPE" envDefault:"application/json" json:"content_type"`             // Content type for POST request
	Headers                 string `env:"FAILURE_TARGET_HTTP_HEADERS" json:"headers"`                                                     // Optional headers to add to the request, provided as a JSON of string key-value pairs. eg: `{"Max Forwards": "10", "Accept-Language": "en-US,en-IE", "Accept-Datetime": "Thu, 31 May 2007 20:35:00 GMT"}`
	BasicAuthUsername       string `env:"FAILURE_TARGET_HTTP_BASICAUTH_USERNAME" json:"basic_auth_username"`                              // Optional basicauth username
	BasicAuthPassword       string `env:"FAILURE_TARGET_HTTP_BASICAUTH_PASSWORD" json:"basic_auth_password"`                              // Optional basicauth password
	CertFile                string `env:"FAILURE_TARGET_HTTP_TLS_CERT_FILE" json:"cert_file"`                                             // The optional certificate file for client authentication
	KeyFile                 string `env:"FAILURE_TARGET_HTTP_TLS_KEY_FILE" json:"key_file"`                                               // The optional key file for client authentication
	CaFile                  string `env:"FAILURE_TARGET_HTTP_TLS_CA_FILE" json:"ca_file"`                                                 // The optional certificate authority file for TLS client authentication
	SkipVerifyTLS           bool   `env:"FAILURE_TARGET_HTTP_TLS_SKIP_VERIFY_TLS" envDefault:"false" json:"skip_verify_tls,string"`       // Optional skip verifying ssl certificates chain - if certfile and keyfile are not provided, this setting is not applied.
}

// FailureTargetsConfig holds configuration for the available targets
type FailureTargetsConfig struct {
	Kinesis  FailureKinesisTargetConfig  `json:"kinesis"`
	PubSub   FailurePubSubTargetConfig   `json:"pubsub"`
	SQS      FailureSQSTargetConfig      `json:"sqs"`
	Kafka    FailureKafkaTargetConfig    `json:"kafka"`
	EventHub FailureEventHubTargetConfig `json:"eventhub"`
	HTTP     FailureHTTPTargetConfig     `json:"http"`

	// Format defines how the message will be transformed before
	// being sent to the target
	Format string `env:"FAILURE_TARGETS_FORMAT" envDefault:"snowplow" json:"format"`
}

// ---------- [ SOURCES ] ----------

// KinesisSourceConfig configures the source for records pulled
type KinesisSourceConfig struct {
	StreamName     string `env:"SOURCE_KINESIS_STREAM_NAME" json:"stream_name"`
	Region         string `env:"SOURCE_KINESIS_REGION" json:"region"`
	RoleARN        string `env:"SOURCE_KINESIS_ROLE_ARN" json:"role_arn"`
	AppName        string `env:"SOURCE_KINESIS_APP_NAME" json:"app_name"`
	StartTimestamp string `env:"SOURCE_KINESIS_START_TIMESTAMP" json:"start_timestamp"` // Timestamp for the kinesis shard iterator to begin processing. Format YYYY-MM-DD HH:MM:SS.MS (miliseconds optional)
}

// PubSubSourceConfig configures the source for records pulled
type PubSubSourceConfig struct {
	ProjectID      string `env:"SOURCE_PUBSUB_PROJECT_ID" json:"project_id"`
	SubscriptionID string `env:"SOURCE_PUBSUB_SUBSCRIPTION_ID" json:"subscription_id"`
}

// SQSSourceConfig configures the source for records pulled
type SQSSourceConfig struct {
	QueueName string `env:"SOURCE_SQS_QUEUE_NAME" json:"queue_name"`
	Region    string `env:"SOURCE_SQS_REGION" json:"region"`
	RoleARN   string `env:"SOURCE_SQS_ROLE_ARN" json:"role_arn"`
}

// SourcesConfig holds configuration for the available sources
type SourcesConfig struct {
	Kinesis KinesisSourceConfig `json:"kinesis"`
	PubSub  PubSubSourceConfig  `json:"pubsub"`
	SQS     SQSSourceConfig     `json:"sqs"`

	// ConcurrentWrites is how many go-routines a source can leverage to parallelise processing
	ConcurrentWrites int `env:"SOURCE_CONCURRENT_WRITES" envDefault:"50" json:"concurrent_writes,string"`
}

// ---------- [ OBSERVABILITY ] ----------

// SentryConfig configures the Sentry error tracker
type SentryConfig struct {
	Dsn   string `env:"SENTRY_DSN" json:"dsn"`
	Tags  string `env:"SENTRY_TAGS" envDefault:"{}" json:"tags"`
	Debug bool   `env:"SENTRY_DEBUG" envDefault:"false" json:"debug,string"`
}

// StatsDStatsReceiverConfig configures the stats metrics receiver
type StatsDStatsReceiverConfig struct {
	Address string `env:"STATS_RECEIVER_STATSD_ADDRESS" json:"address"`
	Prefix  string `env:"STATS_RECEIVER_STATSD_PREFIX" envDefault:"snowplow.stream-replicator" json:"prefix"`
	Tags    string `env:"STATS_RECEIVER_STATSD_TAGS" envDefault:"{}" json:"tags"`
}

// StatsReceiversConfig holds configuration for different stats receivers
type StatsReceiversConfig struct {
	StatsD StatsDStatsReceiverConfig `json:"statsd"`

	// TimeoutSec is how long the observer will wait for a new result before looping
	TimeoutSec int `env:"STATS_RECEIVER_TIMEOUT_SEC" envDefault:"1" json:"timeout_sec,string"`

	// BufferSec is how long the observer buffers results before pushing results out and resetting
	BufferSec int `env:"STATS_RECEIVER_BUFFER_SEC" envDefault:"15" json:"buffer_sec,string"`
}

// Config for holding all configuration details
type Config struct {
	Source         string               `env:"SOURCE" envDefault:"stdin" json:"source_name"`
	Sources        SourcesConfig        `json:"source_config"`
	Target         string               `env:"TARGET" envDefault:"stdout" json:"target_name"`
	Targets        TargetsConfig        `json:"target_config"`
	FailureTarget  string               `env:"FAILURE_TARGET" envDefault:"stdout" json:"failure_target_name"`
	FailureTargets FailureTargetsConfig `json:"failure_target_config"`
	Transformation string               `env:"MESSAGE_TRANSFORMATION" envDefault:"none" json:"message_transformation"`
	LogLevel       string               `env:"LOG_LEVEL" envDefault:"info" json:"log_level"`
	Sentry         SentryConfig         `json:"sentry"`
	StatsReceiver  string               `env:"STATS_RECEIVER" json:"stats_receiver_name"`
	StatsReceivers StatsReceiversConfig `json:"stats_receiver_config"`

	// Provides the ability to provide a GCP service account to the application directly
	GoogleServiceAccountB64 string `env:"GOOGLE_APPLICATION_CREDENTIALS_B64" json:"google_application_credentials"`
}

// defaultConfig returns a pointer to a Config struct value initialized with
// all the default options. This function is used to provide defaults when
// parsing an encoded configuration.
func defaultConfig() *Config {
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

	return &defConfig
}

// NewConfig returns a configuration
func NewConfig() (*Config, error) {
	filename := os.Getenv("STREAM_REPLICATOR_CONFIG_FILE")
	if filename == "" {
		cfg := &Config{}
		err := EnvDecode(cfg)
		if err != nil {
			return nil, err
		}
		return cfg, nil
	}

	switch suffix := strings.ToLower(filepath.Ext(filename)); suffix {
	case ".hocon":
		var err error
		var src []byte

		src, err = os.ReadFile(filename)
		if err != nil {
			return nil, err
		}

		cfg := defaultConfig()
		err = HoconDecode(src, cfg)

		if err != nil {
			return nil, err
		}
		return cfg, nil
	default:
		return nil, errors.New("Invalid extension for the configuration file.")
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
		return target.NewHTTPTarget(
			c.Targets.HTTP.HTTPURL,
			c.Targets.HTTP.RequestTimeoutInSeconds,
			c.Targets.HTTP.ByteLimit,
			c.Targets.HTTP.ContentType,
			c.Targets.HTTP.Headers,
			c.Targets.HTTP.BasicAuthUsername,
			c.Targets.HTTP.BasicAuthPassword,
			c.Targets.HTTP.CertFile,
			c.Targets.HTTP.KeyFile,
			c.Targets.HTTP.CaFile,
			c.Targets.HTTP.SkipVerifyTLS,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got '%s'", c.Target))
	}
}

// GetFailureTarget builds and returns the target that is configured
func (c *Config) GetFailureTarget(AppName string, AppVersion string) (failureiface.Failure, error) {
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
		t, err = target.NewHTTPTarget(
			c.FailureTargets.HTTP.HTTPURL,
			c.FailureTargets.HTTP.requestTimeoutInSeconds,
			c.FailureTargets.HTTP.byteLimit,
			c.FailureTargets.HTTP.ContentType,
			c.FailureTargets.HTTP.Headers,
			c.FailureTargets.HTTP.BasicAuthUsername,
			c.FailureTargets.HTTP.BasicAuthPassword,
			c.FailureTargets.HTTP.CertFile,
			c.FailureTargets.HTTP.KeyFile,
			c.FailureTargets.HTTP.CaFile,
			c.FailureTargets.HTTP.SkipVerifyTLS,
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

// GetTags returns a list of tags to use in identifying this instance of stream-replicator with enough
// entropy so as to avoid collisions as it should not be possible to have both the host and process_id be
// the same.
func (c *Config) GetTags() (map[string]string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get server hostname as tag")
	}

	processID := os.Getpid()

	tags := map[string]string{
		"host":       hostname,
		"process_id": strconv.Itoa(processID),
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
