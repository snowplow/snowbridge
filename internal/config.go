// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package internal

import (
	"fmt"
	"github.com/caarlos0/env/v6"
	"github.com/pkg/errors"
	"time"

	"github.com/snowplow-devops/stream-replicator/internal/failure"
	"github.com/snowplow-devops/stream-replicator/internal/failure/failureiface"
	"github.com/snowplow-devops/stream-replicator/internal/observer"
	"github.com/snowplow-devops/stream-replicator/internal/source"
	"github.com/snowplow-devops/stream-replicator/internal/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/internal/statsreceiver"
	"github.com/snowplow-devops/stream-replicator/internal/statsreceiver/statsreceiveriface"
	"github.com/snowplow-devops/stream-replicator/internal/target"
	"github.com/snowplow-devops/stream-replicator/internal/target/targetiface"
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
	ProjectID         string `env:"TARGET_PUBSUB_PROJECT_ID"`
	TopicName         string `env:"TARGET_PUBSUB_TOPIC_NAME"`
	ServiceAccountB64 string `env:"TARGET_PUBSUB_SERVICE_ACCOUNT_B64"`
}

// SQSTargetConfig configures the destination for records consumed
type SQSTargetConfig struct {
	QueueName string `env:"TARGET_SQS_QUEUE_NAME"`
	Region    string `env:"TARGET_SQS_REGION"`
	RoleARN   string `env:"TARGET_SQS_ROLE_ARN"`
}

// TargetsConfig holds configuration for the available targets
type TargetsConfig struct {
	Kinesis KinesisTargetConfig
	PubSub  PubSubTargetConfig
	SQS     SQSTargetConfig
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
	ProjectID         string `env:"FAILURE_TARGET_PUBSUB_PROJECT_ID"`
	TopicName         string `env:"FAILURE_TARGET_PUBSUB_TOPIC_NAME"`
	ServiceAccountB64 string `env:"FAILURE_TARGET_PUBSUB_SERVICE_ACCOUNT_B64"`
}

// FailureSQSTargetConfig configures the destination for records consumed
type FailureSQSTargetConfig struct {
	QueueName string `env:"FAILURE_TARGET_SQS_QUEUE_NAME"`
	Region    string `env:"FAILURE_TARGET_SQS_REGION"`
	RoleARN   string `env:"FAILURE_TARGET_SQS_ROLE_ARN"`
}

// FailureTargetsConfig holds configuration for the available targets
type FailureTargetsConfig struct {
	Kinesis FailureKinesisTargetConfig
	PubSub  FailurePubSubTargetConfig
	SQS     FailureSQSTargetConfig

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
	ProjectID         string `env:"SOURCE_PUBSUB_PROJECT_ID"`
	SubscriptionID    string `env:"SOURCE_PUBSUB_SUBSCRIPTION_ID"`
	ServiceAccountB64 string `env:"SOURCE_PUBSUB_SERVICE_ACCOUNT_B64"`
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
	// NOTE: PubSub does not use this setting as concurreny is managed internally
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
			c.Sources.PubSub.ProjectID,
			c.Sources.PubSub.SubscriptionID,
			c.Sources.PubSub.ServiceAccountB64,
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
			c.Targets.PubSub.ServiceAccountB64,
		)
	case "sqs":
		return target.NewSQSTarget(
			c.Targets.SQS.Region,
			c.Targets.SQS.QueueName,
			c.Targets.SQS.RoleARN,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs' and got '%s'", c.Target))
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
			c.FailureTargets.PubSub.ServiceAccountB64,
		)
	case "sqs":
		t, err = target.NewSQSTarget(
			c.FailureTargets.SQS.Region,
			c.FailureTargets.SQS.QueueName,
			c.FailureTargets.SQS.RoleARN,
		)
	default:
		err = errors.New(fmt.Sprintf("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs' and got '%s'", c.FailureTarget))
	}
	if err != nil {
		return nil, err
	}

	switch c.FailureTargets.Format {
	case "snowplow":
		return failure.NewSnowplowFailure(t)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid failure format found; expected one of 'snowplow' and got '%s'", c.FailureTargets.Format))
	}
}

// GetObserver builds and returns the observer with the embedded
// optional stats receiver
func (c *Config) GetObserver() (*observer.Observer, error) {
	sr, err := c.GetStatsReceiver()
	if err != nil {
		return nil, err
	}
	return observer.New(sr, time.Duration(c.StatsReceivers.TimeoutSec)*time.Second, time.Duration(c.StatsReceivers.BufferSec)*time.Second), nil
}

// GetStatsReceiver builds and returns the stats receiver
func (c *Config) GetStatsReceiver() (statsreceiveriface.StatsReceiver, error) {
	switch c.StatsReceiver {
	case "statsd":
		return statsreceiver.NewStatsDStatsReceiver(
			c.StatsReceivers.StatsD.Address,
			c.StatsReceivers.StatsD.Prefix,
			c.StatsReceivers.StatsD.Tags,
		)
	case "":
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid stats receiver found; expected one of 'statsd' and got '%s'", c.StatsReceiver))
	}
}
