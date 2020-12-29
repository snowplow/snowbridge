// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	"github.com/caarlos0/env/v6"
	"time"
)

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
	ConcurrentWrites int `env:"SOURCE_CONCURRENT_WRITES" envDefault:"50"`
}

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
func (c *Config) GetSource() (Source, error) {
	switch c.Source {
	case "stdin":
		return NewStdinSource(
			c.Sources.ConcurrentWrites,
		)
	case "kinesis":
		return NewKinesisSource(
			c.Sources.ConcurrentWrites,
			c.Sources.Kinesis.Region,
			c.Sources.Kinesis.StreamName,
			c.Sources.Kinesis.RoleARN,
			c.Sources.Kinesis.AppName,
		)
	case "pubsub":
		return NewPubSubSource(
			c.Sources.PubSub.ProjectID,
			c.Sources.PubSub.SubscriptionID,
			c.Sources.PubSub.ServiceAccountB64,
		)
	case "sqs":
		return NewSQSSource(
			c.Sources.ConcurrentWrites,
			c.Sources.SQS.Region,
			c.Sources.SQS.QueueName,
			c.Sources.SQS.RoleARN,
		)
	default:
		return nil, fmt.Errorf("Invalid source found; expected one of 'stdin, kinesis, pubsub, sqs' and got '%s'", c.Source)
	}
}

// GetTarget builds and returns the target that is configured
func (c *Config) GetTarget() (Target, error) {
	switch c.Target {
	case "stdout":
		return NewStdoutTarget()
	case "kinesis":
		return NewKinesisTarget(c.Targets.Kinesis.Region, c.Targets.Kinesis.StreamName, c.Targets.Kinesis.RoleARN)
	case "pubsub":
		return NewPubSubTarget(c.Targets.PubSub.ProjectID, c.Targets.PubSub.TopicName, c.Targets.PubSub.ServiceAccountB64)
	case "sqs":
		return NewSQSTarget(c.Targets.SQS.Region, c.Targets.SQS.QueueName, c.Targets.SQS.RoleARN)
	default:
		return nil, fmt.Errorf("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs' and got '%s'", c.Target)
	}
}

// GetObserver builds and returns the observer with the embedded
// optional stats receiver
func (c *Config) GetObserver() (*Observer, error) {
	sr, err := c.GetStatsReceiver()
	if err != nil {
		return nil, err
	}
	return NewObserver(sr, time.Duration(c.StatsReceivers.TimeoutSec)*time.Second, time.Duration(c.StatsReceivers.BufferSec)*time.Second), nil
}

// GetStatsReceiver builds and returns the stats receiver
func (c *Config) GetStatsReceiver() (StatsReceiver, error) {
	switch c.StatsReceiver {
	case "statsd":
		return NewStatsDStatsReceiver(c.StatsReceivers.StatsD.Address, c.StatsReceivers.StatsD.Prefix)
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("Invalid stats receiver found; expected one of 'statsd' and got '%s'", c.StatsReceiver)
	}
}
