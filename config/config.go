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
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/failure"
	"github.com/snowplow-devops/stream-replicator/pkg/failure/failureiface"
	"github.com/snowplow-devops/stream-replicator/pkg/observer"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver/statsreceiveriface"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
	"github.com/snowplow-devops/stream-replicator/pkg/target/targetiface"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
)

// Config holds the configuration data along with the decoder to decode them
type Config struct {
	Data    *ConfigurationData
	Decoder Decoder
}

// ConfigurationData for holding all configuration options
type ConfigurationData struct {
	Source                  *Component     `hcl:"source,block" envPrefix:"SOURCE_"`
	Target                  *Component     `hcl:"target,block" envPrefix:"TARGET_"`
	FailureTarget           *FailureConfig `hcl:"failure_target,block"`
	Sentry                  *SentryConfig  `hcl:"sentry,block"`
	StatsReceiver           *StatsConfig   `hcl:"stats_receiver,block"`
	Engines                 []*Component   `hcl:"engine,block"`
	Transformations         []*Component   `hcl:"transform,block"`
	LogLevel                string         `hcl:"log_level,optional" env:"LOG_LEVEL"`
	GoogleServiceAccountB64 string         `hcl:"google_application_credentials_b64,optional" env:"GOOGLE_APPLICATION_CREDENTIALS_B64"`
	UserProvidedID          string         `hcl:"user_provided_id,optional" env:"USER_PROVIDED_ID"`
}

// Component is a type to abstract over configuration blocks.
type Component struct {
	Use *Use `hcl:"use,block"`
}

// Use is a type to denote what a component will be configured to use.
type Use struct {
	Name string   `hcl:",label" env:"NAME"`
	Body hcl.Body `hcl:",remain"`
}

// FailureConfig holds configuration for the failure target.
// It includes the target component to use.
type FailureConfig struct {
	Target *Use   `hcl:"use,block" envPrefix:"FAILURE_TARGET_"`
	Format string `hcl:"format,optional" env:"FAILURE_TARGETS_FORMAT"`
}

// SentryConfig configures the Sentry error tracker.
type SentryConfig struct {
	Dsn   string `hcl:"dsn" env:"SENTRY_DSN"`
	Tags  string `hcl:"tags,optional" env:"SENTRY_TAGS"`
	Debug bool   `hcl:"debug,optional" env:"SENTRY_DEBUG"`
}

// StatsConfig holds configuration for stats receivers.
// It includes a receiver component to use.
type StatsConfig struct {
	Receiver   *Use `hcl:"use,block" envPrefix:"STATS_RECEIVER_"`
	TimeoutSec int  `hcl:"timeout_sec,optional" env:"STATS_RECEIVER_TIMEOUT_SEC"`
	BufferSec  int  `hcl:"buffer_sec,optional" env:"STATS_RECEIVER_BUFFER_SEC"`
}

// defaultConfigData returns the initial main configuration target.
func defaultConfigData() *ConfigurationData {
	return &ConfigurationData{
		Source: &Component{&Use{Name: "stdin"}},
		Target: &Component{&Use{Name: "stdout"}},

		FailureTarget: &FailureConfig{
			Target: &Use{Name: "stdout"},
			Format: "snowplow",
		},
		Sentry: &SentryConfig{
			Tags: "{}",
		},
		StatsReceiver: &StatsConfig{
			Receiver:   &Use{},
			TimeoutSec: 1,
			BufferSec:  15,
		},
		Engines:         nil,
		Transformations: nil,
		LogLevel:        "info",
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
	envDecoder := &EnvDecoder{}

	configData := defaultConfigData()

	err = envDecoder.Decode(decoderOpts, configData)
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
	evalContext := CreateHclContext() // ptr

	// Decoding
	configData := defaultConfigData()
	decoderOpts := &DecoderOptions{Input: fileHCL.Body}
	hclDecoder := &HclDecoder{EvalContext: evalContext}

	err = hclDecoder.Decode(decoderOpts, configData)
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
	componentConfigure := WithDecoderOptions(opts)

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

// GetEngines builds and returns the engines that are configured
func (c *Config) GetEngines() ([]engine.Engine, error) {
	layers := make([]engine.Engine, len(c.Data.Engines))
	for idx, engineComponent := range c.Data.Engines {
		var plug Pluggable
		decoderOpts := &DecoderOptions{
			Input: engineComponent.Use.Body,
		}

		switch engineComponent.Use.Name {
		case `lua`:
			plug = engine.AdaptLuaEngineFunc(engine.LuaEngineConfigFunction)
		case `js`:
			plug = engine.AdaptJSEngineFunc(engine.JSEngineConfigFunction)
		case ``:
			return nil, fmt.Errorf(`engine is missing type`)
		default:
			return nil, fmt.Errorf(`engine type is invalid`)
		}

		component, err := c.CreateComponent(plug, decoderOpts)
		if err != nil {
			return nil, err
		}

		eng, ok := component.(engine.Engine)
		if !ok {
			return nil, errors.New("cannot create engine")
		}
		layers[idx] = eng
	}

	return layers, nil
}

// GetTransformations builds and returns transformationApplyFunction
// from the transformations configured.
func (c *Config) GetTransformations(engines []engine.Engine) (transform.TransformationApplyFunction, error) {
	transformations := make([]*transformconfig.Transformation, len(c.Data.Transformations))
	for idx, transformation := range c.Data.Transformations {
		plug := transformconfig.AdaptTransformationsFunc(transformconfig.TransformationConfigFunction)

		component, err := c.CreateComponent(plug, &DecoderOptions{
			Input: transformation.Use.Body,
		})
		if err != nil {
			return nil, err
		}

		trans, ok := component.(*transformconfig.Transformation)
		if !ok {
			return nil, errors.New(`error parsing transformation`)
		}
		trans.Name = transformation.Use.Name
		transformations[idx] = trans
	}

	validationErrors := transformconfig.ValidateTransformations(transformations)
	for _, err := range validationErrors {
		log.Errorf("validation error: %v", err)
		return nil, errors.New(`transformations validation returned errors`)
	}

	funcs := make([]transform.TransformationFunction, 0, len(transformations))
	for _, transformation := range transformations {
		switch transformation.Name {
		// Builtin transformations
		case "spEnrichedToJson":
			funcs = append(funcs, transform.SpEnrichedToJSON)
		case "spEnrichedSetPk":
			funcs = append(funcs, transform.NewSpEnrichedSetPkFunction(transformation.Option))
		case "spEnrichedFilter":
			filterFunc, err := transform.NewSpEnrichedFilterFunction(transformation.Field, transformation.Regex)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterContext":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionContext(transformation.Field, transformation.Regex)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterUnstructEvent":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionUnstructEvent(transformation.Field, transformation.Regex)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		// Custom transformations
		case "lua":
			luaFunc, err := transformconfig.MkEngineFunction(engines, transformation)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, luaFunc)
		case "js":
			jsFunc, err := transformconfig.MkEngineFunction(engines, transformation)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, jsFunc)
		}
	}

	return transform.NewTransformation(funcs...), nil
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
	return observer.New(sr, time.Duration(c.Data.StatsReceiver.TimeoutSec)*time.Second, time.Duration(c.Data.StatsReceiver.BufferSec)*time.Second), nil
}

// GetStatsReceiver builds and returns the stats receiver
func (c *Config) GetStatsReceiver(tags map[string]string) (statsreceiveriface.StatsReceiver, error) {
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
