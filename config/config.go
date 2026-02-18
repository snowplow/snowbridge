/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package config

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/snowplow/snowbridge/v3/pkg/failure"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/pkg/errors"
	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/monitoring"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	"github.com/snowplow/snowbridge/v3/pkg/statsreceiver"
	"github.com/snowplow/snowbridge/v3/pkg/statsreceiver/statsreceiveriface"
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
	Data    *ConfigurationData
	Decoder Decoder
}

// ConfigurationData for holding all configuration options
type ConfigurationData struct {
	Source           *component        `hcl:"source,block"`
	Target           *TargetConfig     `hcl:"target,block"`
	FailureTarget    *TargetConfig     `hcl:"failure_target,block"`
	FilterTarget     *TargetConfig     `hcl:"filter_target,block"`
	FailureParser    *failureParser    `hcl:"failure_parser,block"`
	Sentry           *sentryConfig     `hcl:"sentry,block"`
	StatsReceiver    *statsConfig      `hcl:"stats_receiver,block"`
	Transform        *TransformConfig  `hcl:"transform,block"`
	LogLevel         string            `hcl:"log_level,optional"`
	UserProvidedID   string            `hcl:"user_provided_id,optional"`
	DisableTelemetry bool              `hcl:"disable_telemetry,optional"`
	License          *licenseConfig    `hcl:"license,block"`
	Retry            *RetryConfig      `hcl:"retry,block"`
	Metrics          *metricsConfig    `hcl:"metrics,block"`
	Monitoring       *monitoringConfig `hcl:"monitoring,block"`
}

// component is a type to abstract over configuration blocks.
type component struct {
	Use *use `hcl:"use,block"`
}

// TransformConfig holds configuration for transformations.
type TransformConfig struct {
	Transformations []*use `hcl:"use,block"`
	WorkerPool      int    `hcl:"worker_pool,optional"`
}

// use is a type to denote what a component will be configured to use.
type use struct {
	Name string   `hcl:",label"`
	Body hcl.Body `hcl:",remain"`
}

// TargetConfig is handled in the target package.
type TargetConfig struct {
	Target *use `hcl:"use,block"`
}

// failureParser holds configuration for failure handling.
type failureParser struct {
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

type RetryConfig struct {
	Transient *TransientRetryConfig `hcl:"transient,block"`
	Setup     *SetupRetryConfig     `hcl:"setup,block"`
	Throttle  *ThrottleRetryConfig  `hcl:"throttle,block"`
}

type metricsConfig struct {
	E2ELatencyEnabled            bool `hcl:"enable_e2e_latency,optional"`
	KinsumerMemoryMetricsEnabled bool `hcl:"enable_kinsumer_memory_metrics,optional"`
}

type monitoringConfig struct {
	Webhook          *webhookConfig          `hcl:"webhook,block"`
	MetadataReporter *metadataReporterConfig `hcl:"metadata_reporter,block"`
}

type webhookConfig struct {
	Endpoint          string            `hcl:"endpoint"`
	Tags              map[string]string `hcl:"tags,optional"`
	HeartbeatInterval int               `hcl:"heartbeat_interval_seconds,optional"`
}

type metadataReporterConfig struct {
	Endpoint string            `hcl:"endpoint"`
	Tags     map[string]string `hcl:"tags,optional"`
}

type TransientRetryConfig struct {
	Delay           int  `hcl:"delay_ms,optional"`
	MaxAttempts     int  `hcl:"max_attempts,optional"`
	InvalidAfterMax bool `hcl:"invalid_after_max,optional"` // default: false
}

type SetupRetryConfig struct {
	Delay           int  `hcl:"delay_ms,optional"`
	MaxAttempts     int  `hcl:"max_attempts,optional"`
	InvalidAfterMax bool `hcl:"invalid_after_max,optional"` // default: false
}

type ThrottleRetryConfig struct {
	Delay           int  `hcl:"delay_ms,optional"`
	MaxAttempts     int  `hcl:"max_attempts,optional"`
	InvalidAfterMax bool `hcl:"invalid_after_max,optional"` // default: false
}

// defaultConfigData returns the initial main configuration target.
func defaultConfigData() *ConfigurationData {
	return &ConfigurationData{
		Source:        &component{&use{Name: "stdin"}},
		Target:        &TargetConfig{Target: &use{Name: "stdout"}},
		FailureTarget: &TargetConfig{Target: &use{Name: "stdout"}},
		FilterTarget:  &TargetConfig{Target: &use{Name: "silent"}},
		FailureParser: &failureParser{
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
		Transform: &TransformConfig{
			Transformations: nil,
			WorkerPool:      0, // 0 means use default (runtime.GOMAXPROCS(0) + 1)
		},
		LogLevel:         "info",
		DisableTelemetry: false,
		License: &licenseConfig{
			Accept: false,
		},
		Retry: &RetryConfig{
			Transient: &TransientRetryConfig{
				Delay:       1000,
				MaxAttempts: 5,
			},
			Setup: &SetupRetryConfig{
				Delay:       20000,
				MaxAttempts: 5,
			},
			Throttle: &ThrottleRetryConfig{
				Delay:       10000,
				MaxAttempts: 5,
			},
		},
		Metrics: &metricsConfig{
			E2ELatencyEnabled:            false,
			KinsumerMemoryMetricsEnabled: false,
		},
		Monitoring: &monitoringConfig{
			Webhook: &webhookConfig{
				Tags:              map[string]string{},
				HeartbeatInterval: 300,
			},
			MetadataReporter: &metadataReporterConfig{
				Tags: map[string]string{},
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
		return NewHclConfig(src, filename)
	}
}

func NewHclConfig(fileContents []byte, filename string) (*Config, error) {

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
func (c *Config) CreateComponent(p Pluggable, opts *DecoderOptions) (any, error) {
	componentConfigure := withDecoderOptions(opts)

	decodedConfig, err := componentConfigure(p, c.Decoder)
	if err != nil {
		return nil, err
	}

	return p.Create(decodedConfig)
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

func (c *Config) GetFailureParser(failureTargetMaxMessageSize int, appName, appVersion string) (failure.FailureParser, error) {
	switch c.Data.FailureParser.Format {
	case failure.SnowplowFailureTarget:
		return failure.NewSnowplowFailure(failureTargetMaxMessageSize, appName, appVersion)
	case failure.EventForwardingFailureTarget:
		return failure.NewEventForwardingFailure(failureTargetMaxMessageSize, appName, appVersion)
	default:
		return nil, fmt.Errorf("invalid failure format found; expected one of 'snowplow', 'event_forwarding' and got '%s'", c.Data.FailureParser.Format)
	}
}

// GetObserver builds and returns the observer with the embedded
// optional stats receiver & metadata reporter
func (c *Config) GetObserver(appName, appVersion string, tags map[string]string) (*observer.Observer, error) {
	sr, err := c.getStatsReceiver(tags)
	if err != nil {
		return nil, err
	}

	metadataReporter, err := c.getMetadataReporter(appName, appVersion)
	if err != nil {
		return nil, err
	}

	return observer.New(sr, time.Duration(c.Data.StatsReceiver.TimeoutSec)*time.Second, time.Duration(c.Data.StatsReceiver.BufferSec)*time.Second, metadataReporter), nil
}

func (c *Config) GetWebhookMonitoring(appName, appVersion string) (*monitoring.WebhookMonitoring, chan error, error) {
	if c.Data.Monitoring.Webhook.Endpoint == "" {
		return nil, nil, nil
	}

	if err := common.CheckURL(c.Data.Monitoring.Webhook.Endpoint); err != nil {
		return nil, nil, err
	}

	alertChan := make(chan error)

	client := http.DefaultClient
	endpoint := c.Data.Monitoring.Webhook.Endpoint
	tags := c.Data.Monitoring.Webhook.Tags
	heartbeatInterval := time.Duration(c.Data.Monitoring.Webhook.HeartbeatInterval) * time.Second

	return monitoring.NewWebhookMonitoring(appName, appVersion, client, endpoint, tags, heartbeatInterval, alertChan), alertChan, nil
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
			statsreceiver.NewStatsDReceiverWithTags(tags, c.Data.Metrics.E2ELatencyEnabled, c.Data.Metrics.KinsumerMemoryMetricsEnabled),
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

func (c *Config) getMetadataReporter(appName, appVersion string) (monitoring.MetadataReporterer, error) {
	if c.Data.Monitoring.MetadataReporter.Endpoint == "" {
		return nil, nil
	}

	if err := common.CheckURL(c.Data.Monitoring.MetadataReporter.Endpoint); err != nil {
		return nil, err
	}

	client := http.DefaultClient
	endpoint := c.Data.Monitoring.MetadataReporter.Endpoint
	tags := c.Data.Monitoring.MetadataReporter.Tags

	return monitoring.NewMetadataReporter(appName, appVersion, client, endpoint, tags), nil
}
