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

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
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

// Config holds the configuration data along with the decoder to decode them
type Config struct {
	Data    *configurationData
	Decoder decoder
}

// configurationData for holding all configuration options
type configurationData struct {
	Source                  *component     `hcl:"source,block" envPrefix:"SOURCE_"`
	Target                  *component     `hcl:"target,block" envPrefix:"TARGET_"`
	FailureTarget           *failureConfig `hcl:"failure_target,block"`
	Sentry                  *sentryConfig  `hcl:"sentry,block"`
	StatsReceiver           *statsConfig   `hcl:"stats_receiver,block"`
	Transformation          string         `hcl:"message_transformation,optional" env:"MESSAGE_TRANSFORMATION"`
	LogLevel                string         `hcl:"log_level,optional" env:"LOG_LEVEL"`
	GoogleServiceAccountB64 string         `hcl:"google_application_credentials_b64,optional" env:"GOOGLE_APPLICATION_CREDENTIALS_B64"`
	UserProvidedID          string         `hcl:"user_provided_id,optional" env:"USER_PROVIDED_ID"`
}

// component is a type to abstract over configuration blocks.
type component struct {
	Use *use `hcl:"use,block"`
}

// use is a type to denote what a component will be configured to use.
type use struct {
	Name string   `hcl:",label" env:"NAME"`
	Body hcl.Body `hcl:",remain"`
}

// failureConfig holds configuration for the failure target.
// It includes the target component to use.
type failureConfig struct {
	Target *use   `hcl:"use,block" envPrefix:"FAILURE_TARGET_"`
	Format string `hcl:"format,optional" env:"FAILURE_TARGETS_FORMAT"`
}

// sentryConfig configures the Sentry error tracker.
type sentryConfig struct {
	Dsn   string `hcl:"dsn" env:"SENTRY_DSN"`
	Tags  string `hcl:"tags,optional" env:"SENTRY_TAGS"`
	Debug bool   `hcl:"debug,optional" env:"SENTRY_DEBUG"`
}

// statsConfig holds configuration for stats receivers.
// It includes a receiver component to use.
type statsConfig struct {
	Receiver   *use `hcl:"use,block" envPrefix:"STATS_RECEIVER_"`
	TimeoutSec int  `hcl:"timeout_sec,optional" env:"STATS_RECEIVER_TIMEOUT_SEC"`
	BufferSec  int  `hcl:"buffer_sec,optional" env:"STATS_RECEIVER_BUFFER_SEC"`
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
		Transformation: "none",
		LogLevel:       "info",
	}
}

// NewConfig returns a configuration
func NewConfig() (*Config, error) {
	filename := os.Getenv("STREAM_REPLICATOR_CONFIG_FILE")
	if filename == "" {
		return newEnvConfig()
	}

	switch suffix := strings.ToLower(filepath.Ext(filename)); suffix {
	case ".hcl":
		return newHclConfig(filename)
	default:
		return nil, errors.New("invalid extension for the configuration file")
	}
}

func newEnvConfig() (*Config, error) {
	var err error

	decoderOpts := &DecoderOptions{}
	envDecoder := &envDecoder{}

	configData := defaultConfigData()

	err = envDecoder.decode(decoderOpts, configData)
	if err != nil {
		return nil, err
	}

	mainConfig := Config{
		Data:    configData,
		Decoder: envDecoder,
	}

	return &mainConfig, nil
}

func newHclConfig(filename string) (*Config, error) {
	src, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// Parsing
	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(src, filename)
	if diags.HasErrors() {
		return nil, diags
	}

	// Creating EvalContext
	evalContext := createHclContext() // ptr

	// Decoding
	configData := defaultConfigData()
	decoderOpts := &DecoderOptions{Input: fileHCL.Body}
	hclDecoder := &hclDecoder{EvalContext: evalContext}

	err = hclDecoder.decode(decoderOpts, configData)
	if err != nil {
		return nil, err
	}

	mainConfig := Config{
		Data:    configData,
		Decoder: hclDecoder,
	}

	return &mainConfig, nil
}

// CreateComponent creates a pluggable component given the decoder options.
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
			target.NewStdoutTarget,
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
			target.NewEventHubTarget,
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
		Prefix: "FAILURE_",
		Input:  useFailureTarget.Body,
	}

	switch useFailureTarget.Name {
	case "stdout":
		plug = target.AdaptStdoutTargetFunc(
			target.NewStdoutTarget,
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
			target.NewEventHubTarget,
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

// GetTransformations builds and returns transformationApplyFunction from the transformations configured
func (c *Config) GetTransformations() (transform.TransformationApplyFunction, error) {
	funcs := make([]transform.TransformationFunction, 0, 0)

	// Parse list of transformations
	transformations := strings.Split(c.Data.Transformation, ",")

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
		case "spEnrichedFilterContext":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionContext(funcOpts[1])
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterUnstructEvent":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionUnstructEvent(funcOpts[1])
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "none":
		default:
			return nil, errors.New(fmt.Sprintf("Invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk:{option}', spEnrichedFilter:{option} and got '%s'", c.Data.Transformation))
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
