// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

// KinesisTargetConfig configures the destination for records consumed
type KinesisTargetConfig struct {
	StreamName string
	Region     string
	RoleARN    string
}

// PubSubTargetConfig configures the destination for records consumed
type PubSubTargetConfig struct {
	ProjectID         string
	TopicName         string
	ServiceAccountB64 string
}

// TargetsConfig holds configuration for the available targets
type TargetsConfig struct {
	Kinesis KinesisTargetConfig
	PubSub  PubSubTargetConfig
}

// KinesisSourceConfig configures the source for records pulled
type KinesisSourceConfig struct {
	StreamName string
	Region     string
	RoleARN    string
	AppName    string
}

// PubSubSourceConfig configures the source for records pulled
type PubSubSourceConfig struct {
	ProjectID         string
	SubscriptionID    string
	ServiceAccountB64 string
}

// SQSSourceConfig configures the source for records pulled
type SQSSourceConfig struct {
	QueueName string
	Region    string
	RoleARN   string
}

// SourcesConfig holds configuration for the available sources
type SourcesConfig struct {
	Kinesis KinesisSourceConfig
	PubSub  PubSubSourceConfig
	SQS     SQSSourceConfig
}

// SentryConfig configures the Sentry error tracker
type SentryConfig struct {
	Dsn   string
	Tags  string
	Debug bool
}

// Config for holding all configuration details
type Config struct {
	Source   string
	Target   string
	LogLevel string
	Sentry   SentryConfig
	Sources  SourcesConfig
	Targets  TargetsConfig
}

// NewConfig resolves the config from the environment
func NewConfig() *Config {
	var defaultConfig = &Config{
		Source:   "stdin",
		Target:   "stdout",
		LogLevel: "info",
		Sentry: SentryConfig{
			Tags:  "{}",
			Debug: false,
		},
	}

	// Override values from environment
	return configFromEnv(defaultConfig)
}

// configFromEnv loads the config struct from environment variables
func configFromEnv(c *Config) *Config {
	return &Config{
		Source:   getEnvOrElse("SOURCE", c.Source),
		Target:   getEnvOrElse("TARGET", c.Target),
		LogLevel: getEnvOrElse("LOG_LEVEL", c.LogLevel),
		Sentry: SentryConfig{
			Dsn:   getEnvOrElse("SENTRY_DSN", c.Sentry.Dsn),
			Tags:  getEnvOrElse("SENTRY_TAGS", c.Sentry.Tags),
			Debug: getEnvBoolOrElse("SENTRY_DEBUG", c.Sentry.Debug),
		},
		Sources: SourcesConfig{
			Kinesis: KinesisSourceConfig{
				StreamName: getEnvOrElse("SOURCE_KINESIS_STREAM_NAME", c.Sources.Kinesis.StreamName),
				Region:     getEnvOrElse("SOURCE_KINESIS_REGION", c.Sources.Kinesis.Region),
				RoleARN:    getEnvOrElse("SOURCE_KINESIS_ROLE_ARN", c.Sources.Kinesis.RoleARN),
				AppName:    getEnvOrElse("SOURCE_KINESIS_APP_NAME", c.Sources.Kinesis.AppName),
			},
			PubSub: PubSubSourceConfig{
				ProjectID:         getEnvOrElse("SOURCE_PUBSUB_PROJECT_ID", c.Sources.PubSub.ProjectID),
				SubscriptionID:    getEnvOrElse("SOURCE_PUBSUB_SUBSCRIPTION_ID", c.Sources.PubSub.SubscriptionID),
				ServiceAccountB64: getEnvOrElse("SOURCE_PUBSUB_SERVICE_ACCOUNT_B64", c.Sources.PubSub.ServiceAccountB64),
			},
			SQS: SQSSourceConfig{
				QueueName: getEnvOrElse("SOURCE_SQS_QUEUE_NAME", c.Sources.SQS.QueueName),
				Region:    getEnvOrElse("SOURCE_SQS_REGION", c.Sources.SQS.Region),
				RoleARN:   getEnvOrElse("SOURCE_SQS_ROLE_ARN", c.Sources.SQS.RoleARN),
			},
		},
		Targets: TargetsConfig{
			Kinesis: KinesisTargetConfig{
				StreamName: getEnvOrElse("TARGET_KINESIS_STREAM_NAME", c.Targets.Kinesis.StreamName),
				Region:     getEnvOrElse("TARGET_KINESIS_REGION", c.Targets.Kinesis.Region),
				RoleARN:    getEnvOrElse("TARGET_KINESIS_ROLE_ARN", c.Targets.Kinesis.RoleARN),
			},
			PubSub: PubSubTargetConfig{
				ProjectID:         getEnvOrElse("TARGET_PUBSUB_PROJECT_ID", c.Targets.PubSub.ProjectID),
				TopicName:         getEnvOrElse("TARGET_PUBSUB_TOPIC_NAME", c.Targets.PubSub.TopicName),
				ServiceAccountB64: getEnvOrElse("TARGET_PUBSUB_SERVICE_ACCOUNT_B64", c.Targets.PubSub.ServiceAccountB64),
			},
		},
	}
}

// GetSource builds and returns the source that is configured
func (c *Config) GetSource() (Source, error) {
	if c.Source == "stdin" {
		return NewStdinSource()
	} else if c.Source == "kinesis" {
		return NewKinesisSource(c.Sources.Kinesis.Region, c.Sources.Kinesis.StreamName, c.Sources.Kinesis.RoleARN, c.Sources.Kinesis.AppName)
	} else if c.Source == "pubsub" {
		return NewPubSubSource(c.Sources.PubSub.ProjectID, c.Sources.PubSub.SubscriptionID, c.Sources.PubSub.ServiceAccountB64)
	} else if c.Source == "sqs" {
		return NewSQSSource(c.Sources.SQS.Region, c.Sources.SQS.QueueName, c.Sources.SQS.RoleARN)
	} else {
		return nil, fmt.Errorf("Invalid source found; expected one of 'stdin, kinesis, pubsub, sqs' and got '%s'", c.Source)
	}
}

// GetTarget builds and returns the target that is configured
func (c *Config) GetTarget() (Target, error) {
	if c.Target == "stdout" {
		return NewStdoutTarget()
	} else if c.Target == "kinesis" {
		return NewKinesisTarget(c.Targets.Kinesis.Region, c.Targets.Kinesis.StreamName, c.Targets.Kinesis.RoleARN)
	} else if c.Target == "pubsub" {
		return NewPubSubTarget(c.Targets.PubSub.ProjectID, c.Targets.PubSub.TopicName, c.Targets.PubSub.ServiceAccountB64)
	} else {
		return nil, fmt.Errorf("Invalid target found; expected one of 'stdout, kinesis, pubsub' and got '%s'", c.Target)
	}
}

// --- HELPERS

// getEnvOrElse returns an environment variable value or a default
func getEnvOrElse(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

// getEnvBoolOrElse returns an environment variable value and casts it to a boolean or passes a default
func getEnvBoolOrElse(key string, defaultVal bool) bool {
	if value, exists := os.LookupEnv(key); exists {
		mValue, err := strconv.ParseBool(value)
		if err != nil {
			log.Error(fmt.Sprintf("Error converting string to bool for key %s: %s; using default '%v'", key, err.Error(), defaultVal))
			return defaultVal
		}
		return mValue
	}
	return defaultVal
}
