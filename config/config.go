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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/pkg/errors"
	"github.com/snowplow/snowbridge/pkg/failure"
	"github.com/snowplow/snowbridge/pkg/failure/failureiface"
	"github.com/snowplow/snowbridge/pkg/observer"
	"github.com/snowplow/snowbridge/pkg/statsreceiver"
	"github.com/snowplow/snowbridge/pkg/statsreceiver/statsreceiveriface"
	"github.com/snowplow/snowbridge/pkg/target"
	"github.com/snowplow/snowbridge/pkg/target/targetiface"
)

// ConfigurationPair allows modular packages to define their own configuration and function to interpret the configuration.
// This allows us to interpret configurations without needing to import those packages, and allows the packages to define
// how their own configurations should be interpreted.
type ConfigurationPair struct {
	Name   string
	Handle Pluggable
}

// Config holds the configuration data along with the Decoder to Decode them
type Config struct {
	Data    *configurationData
	Decoder Decoder
}

// configurationData for holding all configuration options
type configurationData struct {
	Source           *component     `hcl:"source,block"`
	Target           *component     `hcl:"target,block"`
	FailureTarget    *failureConfig `hcl:"failure_target,block"`
	Sentry           *sentryConfig  `hcl:"sentry,block"`
	StatsReceiver    *statsConfig   `hcl:"stats_receiver,block"`
	Transformations  []*component   `hcl:"transform,block"`
	LogLevel         string         `hcl:"log_level,optional"`
	UserProvidedID   string         `hcl:"user_provided_id,optional"`
	DisableTelemetry bool           `hcl:"disable_telemetry,optional"`
	License          *licenseConfig `hcl:"license,block"`
	Retry            *retryConfig   `hcl:"retry,block"`
}

// component is a type to abstract over configuration blocks.
type component struct {
	Use *use `hcl:"use,block"`
}

// use is a type to denote what a component will be configured to use.
type use struct {
	Name string   `hcl:",label"`
	Body hcl.Body `hcl:",remain"`
}

// failureConfig holds configuration for the failure target.
// It includes the target component to use.
type failureConfig struct {
	Target *use   `hcl:"use,block"`
	Format string `hcl:"format,optional"`
}

// sentryConfig configures the Sentry error tracker.
type sentryConfig struct {
	Dsn   string `hcl:"dsn"`
	Tags  string `hcl:"tags,optional"`
	Debug bool   `hcl:"debug,optional"`
}

// statsConfig holds configuration for stats receivers.
// It includes a receiver component to use.
type statsConfig struct {
	Receiver   *use `hcl:"use,block"`
	TimeoutSec int  `hcl:"timeout_sec,optional"`
	BufferSec  int  `hcl:"buffer_sec,optional"`
}

type licenseConfig struct {
	Accept bool `hcl:"accept,optional"`
}

type retryConfig struct {
	Transient *transientRetryConfig `hcl:"transient,block"`
	Setup     *setupRetryConfig     `hcl:"setup,block"`
}

type transientRetryConfig struct {
	Delay       int `hcl:"delay_sec,optional"`
	MaxAttempts int `hcl:"max_attempts,optional"`
}

type setupRetryConfig struct {
	Delay int `hcl:"delay_sec,optional"`
}

// defaultConfigData returns the initial main configuration target.
func defaultConfigData() *configurationData {
	return &configurationData{
		Source: &component{&use{Name: "stdin"}},
		Target: &component{&use{Name: "stdout"}},

		FailureTarget: &failureConfig{
			Target: &use{Name: "stdout"},
			Format: "snowplow",
		},
		Sentry: &sentryConfig{
			Tags: "{}",
		},
		StatsReceiver: &statsConfig{
			Receiver:   &use{},
			TimeoutSec: 1,
			BufferSec:  15,
		},
		Transformations:  nil,
		LogLevel:         "info",
		DisableTelemetry: false,
		License: &licenseConfig{
			Accept: false,
		},
		Retry: &retryConfig{
			Transient: &transientRetryConfig{
				Delay:       1,
				MaxAttempts: 5,
			},
			Setup: &setupRetryConfig{
				Delay: 20,
			},
		},
	}
}

// NewConfig returns a configuration
func NewConfig() (*Config, error) {
	switch filename := os.Getenv("SNOWBRIDGE_CONFIG_FILE"); filename {
	case "":
		return &Config{
			Data:    defaultConfigData(),
			Decoder: &defaultsDecoder{},
		}, nil

	default:
		// read the config file
		src, readErr := os.ReadFile(filename)
		if readErr != nil {
			return nil, readErr
		}
		// Make a config from it
		return newHclConfig(src, filename)
	}
}

func newHclConfig(fileContents []byte, filename string) (*Config, error) {

	// Parsing
	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(fileContents, filename)
	if diags.HasErrors() {
		return nil, diags
	}

	// Creating EvalContext
	evalContext := CreateHclContext() // ptr

	// Decoding
	configData := defaultConfigData()
	decoderOpts := &DecoderOptions{Input: fileHCL.Body}
	hclDecoder := &hclDecoder{EvalContext: evalContext}

	err := hclDecoder.Decode(decoderOpts, configData)
	if err != nil {
		return nil, err
	}

	mainConfig := Config{
		Data:    configData,
		Decoder: hclDecoder,
	}

	return &mainConfig, nil
}

// CreateComponent creates a pluggable component given the Decoder options.
func (c *Config) CreateComponent(p Pluggable, opts *DecoderOptions) (interface{}, error) {
	componentConfigure := withDecoderOptions(opts)

	decodedConfig, err := componentConfigure(p, c.Decoder)
	if err != nil {
		return nil, err
	}

	return p.Create(decodedConfig)
}

// GetTarget builds and returns the target that is configured
func (c *Config) GetTarget() (targetiface.Target, error) {
	var plug Pluggable
	useTarget := c.Data.Target.Use
	decoderOpts := &DecoderOptions{
		Input: useTarget.Body,
	}

	switch useTarget.Name {
	case "stdout":
		plug = target.AdaptStdoutTargetFunc(
			target.StdoutTargetConfigFunction,
		)
	case "kinesis":
		plug = target.AdaptKinesisTargetFunc(
			target.KinesisTargetConfigFunction,
		)
	case "pubsub":
		plug = target.AdaptPubSubTargetFunc(
			target.PubSubTargetConfigFunction,
		)
	case "sqs":
		plug = target.AdaptSQSTargetFunc(
			target.SQSTargetConfigFunction,
		)
	case "kafka":
		plug = target.AdaptKafkaTargetFunc(
			target.NewKafkaTarget,
		)
	case "eventhub":
		plug = target.AdaptEventHubTargetFunc(
			target.EventHubTargetConfigFunction,
		)
	case "http":
		plug = target.AdaptHTTPTargetFunc(
			target.HTTPTargetConfigFunction,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got '%s'", useTarget.Name))
	}

	component, err := c.CreateComponent(plug, decoderOpts)
	if err != nil {
		return nil, err
	}

	if t, ok := component.(targetiface.Target); ok {
		return t, nil
	}

	return nil, fmt.Errorf("could not interpret target configuration for %q", useTarget.Name)
}

// GetFailureTarget builds and returns the target that is configured
func (c *Config) GetFailureTarget(AppName string, AppVersion string) (failureiface.Failure, error) {
	var plug Pluggable
	var err error

	useFailureTarget := c.Data.FailureTarget.Target
	decoderOpts := &DecoderOptions{
		Input: useFailureTarget.Body,
	}

	switch useFailureTarget.Name {
	case "stdout":
		plug = target.AdaptStdoutTargetFunc(
			target.StdoutTargetConfigFunction,
		)
	case "kinesis":
		plug = target.AdaptKinesisTargetFunc(
			target.KinesisTargetConfigFunction,
		)
	case "pubsub":
		plug = target.AdaptPubSubTargetFunc(
			target.PubSubTargetConfigFunction,
		)
	case "sqs":
		plug = target.AdaptSQSTargetFunc(
			target.SQSTargetConfigFunction,
		)
	case "kafka":
		plug = target.AdaptKafkaTargetFunc(
			target.NewKafkaTarget,
		)
	case "eventhub":
		plug = target.AdaptEventHubTargetFunc(
			target.EventHubTargetConfigFunction,
		)
	case "http":
		plug = target.AdaptHTTPTargetFunc(
			target.HTTPTargetConfigFunction,
		)
	default:
		return nil, errors.New(fmt.Sprintf("Invalid failure target found; expected one of 'stdout, kinesis, pubsub, sqs, kafka, eventhub, http' and got '%s'", useFailureTarget.Name))
	}

	component, err := c.CreateComponent(plug, decoderOpts)
	if err != nil {
		return nil, err
	}

	if t, ok := component.(targetiface.Target); ok {
		switch c.Data.FailureTarget.Format {
		case "snowplow":
			return failure.NewSnowplowFailure(t, AppName, AppVersion)
		default:
			return nil, errors.New(fmt.Sprintf("Invalid failure format found; expected one of 'snowplow' and got '%s'", c.Data.FailureTarget.Format))
		}
	}

	return nil, fmt.Errorf("could not interpret failure target configuration for %q", useFailureTarget.Name)
}

// GetTags returns a list of tags to use in identifying this instance of snowbridge with enough
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
	sr, err := c.getStatsReceiver(tags)
	if err != nil {
		return nil, err
	}
	return observer.New(sr, time.Duration(c.Data.StatsReceiver.TimeoutSec)*time.Second, time.Duration(c.Data.StatsReceiver.BufferSec)*time.Second), nil
}

// getStatsReceiver builds and returns the stats receiver
func (c *Config) getStatsReceiver(tags map[string]string) (statsreceiveriface.StatsReceiver, error) {
	useReceiver := c.Data.StatsReceiver.Receiver
	decoderOpts := &DecoderOptions{
		Input: useReceiver.Body,
	}

	switch useReceiver.Name {
	case "statsd":
		plug := statsreceiver.AdaptStatsDStatsReceiverFunc(
			statsreceiver.NewStatsDReceiverWithTags(tags),
		)
		component, err := c.CreateComponent(plug, decoderOpts)
		if err != nil {
			return nil, err
		}

		if r, ok := component.(statsreceiveriface.StatsReceiver); ok {
			return r, nil
		}

		return nil, fmt.Errorf("could not interpret stats receiver configuration for %q", useReceiver.Name)
	case "":
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("Invalid stats receiver found; expected one of 'statsd' and got '%s'", useReceiver.Name))
	}
}
