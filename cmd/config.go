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
	Brokers    string `env:"TARGET_KAFKA_BROKERS"`
	TopicName  string `env:"TARGET_KAFKA_TOPIC_NAME"`
	ByteLimit  int    `env:"TARGET_KAFKA_BYTE_LIMIT"`
	Idempotent bool   `env:"TARGET_KAFKA_IDEMPOTENT"` // Exactly once writes
	certFile   string `env:"TARGET_KAFKA_CERT_FILE"`  // The optional certificate file for client authentication
	keyFile    string `env:"TARGET_KAFKA_KEY_FILE"`   // The optional key file for client authentication
	caCert     string `env:"TARGET_KAFKA_CA_CERT"`    // The optional certificate authority file for TLS client authentication
	verifySsl  bool   `env:"TARGET_KAFKA_VERIFY_SSL"` // Optional verify ssl certificates chain
}

// TargetsConfig holds configuration for the available targets
type TargetsConfig struct {
	Kinesis KinesisTargetConfig
	PubSub  PubSubTargetConfig
	SQS     SQSTargetConfig
	Kafka   KafkaTargetConfig
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

// KafkaTargetConfig configures the destination for records consumed
type FailureKafkaTargetConfig struct {
	Brokers    string `env:"FAILURE_TARGET_KAFKA_BROKERS"`
	TopicName  string `env:"FAILURE_TARGET_KAFKA_TOPIC_NAME"`
	ByteLimit  int    `env:"FAILURE_TARGET_KAFKA_BYTE_LIMIT"`
	Idempotent bool   `env:"FAILURE_TARGET_KAFKA_IDEMPOTENT"` // Exactly once writes
	certFile   string `env:"FAILURE_TARGET_KAFKA_CERT_FILE"`  // The optional certificate file for client authentication
	keyFile    string `env:"FAILURE_TARGET_KAFKA_KEY_FILE"`   // The optional key file for client authentication
	caCert     string `env:"FAILURE_TARGET_KAFKA_CA_CERT"`    // The optional certificate authority file for TLS client authentication
	verifySsl  bool   `env:"FAILURE_TARGET_KAFKA_VERIFY_SSL"` // Optional verify ssl certificates chain
}

// FailureTargetsConfig holds configuration for the available targets
type FailureTargetsConfig struct {
	Kinesis FailureKinesisTargetConfig
	PubSub  FailurePubSubTargetConfig
	SQS     FailureSQSTargetConfig
	Kafka   FailureKafkaTargetConfig

	// Format defines how the message will be transformed before
	// being sent to the target
	Format string `env:"FAILURE_TARGETS_FORMAT" envDefault:"snowplow"`
}

// ---------- [ SOURCES ] ----------

// KinesisSourceConfig configures the source for records pulled
type KinesisSourceConfig struct {
	StreamName string `env:"SOURCE_KINESIS_STREAM_NAME"`
	Region     string `env:"SOURCE_KINESIS_REGION"`
	RoleARN    string `env:"SOURCE_KINESIS_ROLE_ARN"`
	AppName    string `env:"SOURCE_KINESIS_APP_NAME"`
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
		return source.NewKinesisSource(
			c.Sources.ConcurrentWrites,
			c.Sources.Kinesis.Region,
			c.Sources.Kinesis.StreamName,
			c.Sources.Kinesis.RoleARN,
			c.Sources.Kinesis.AppName,
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
		return target.NewKafkaTarget(
			c.Targets.Kafka.Brokers,
			c.Targets.Kafka.TopicName,
			c.Targets.Kafka.ByteLimit,
			c.Targets.Kafka.Idempotent,
			c.Targets.Kafka.certFile,
			c.Targets.Kafka.keyFile,
			c.Targets.Kafka.caCert,
			c.Targets.Kafka.verifySsl,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka' and got '%s'", c.Target))
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
		t, err = target.NewKafkaTarget(
			c.FailureTargets.Kafka.Brokers,
			c.FailureTargets.Kafka.TopicName,
			c.FailureTargets.Kafka.ByteLimit,
			c.FailureTargets.Kafka.Idempotent,
			c.FailureTargets.Kafka.certFile,
			c.FailureTargets.Kafka.keyFile,
			c.FailureTargets.Kafka.caCert,
			c.FailureTargets.Kafka.verifySsl,
		)
	default:
		err = errors.New(fmt.Sprintf("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka' and got '%s'", c.FailureTarget))
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
