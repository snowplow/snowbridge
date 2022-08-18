// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver"
	"github.com/stretchr/testify/assert"
)

type MockStatsDStatsReceiverAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockStatsDStatsReceiverAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockStatsDStatsReceiverAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &statsreceiver.StatsDStatsReceiverConfig{}

	return cfg, nil
}

// AdaptStatsDStatsReceiverFunc returns a StatsDStatsReceiverAdapter.
func MockAdaptStatsDStatsReceiverNoDefaults() MockStatsDStatsReceiverAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*statsreceiver.StatsDStatsReceiverConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StatsDStatsReceiverConfig")
		}

		return cfg, nil
	}
}

func MockAdaptStatsDStatsReceiver() statsreceiver.StatsDStatsReceiverAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*statsreceiver.StatsDStatsReceiverConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StatsDStatsReceiverConfig")
		}

		return cfg, nil
	}
}

// TestStatsdConfigs tests the statsd statsreceiver config examples
// It uses separate mocks to first check the full config with no defaults, and then check the minimal one with defaults.
func TestStatsdConfigs(t *testing.T) {
	assert := assert.New(t)

	hclFilenameFull := filepath.Join("configs", "monitoring", "full", "statsd-full.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilenameFull)

	cfgFull, err := config.NewConfig()
	assert.NotNil(cfgFull)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	// Check the statsD part
	useFull := cfgFull.Data.StatsReceiver.Receiver
	decoderOptsFull := &config.DecoderOptions{
		Input: useFull.Body,
	}

	resultFull, errFull := cfgFull.CreateComponent(MockAdaptStatsDStatsReceiverNoDefaults(), decoderOptsFull)
	if errFull != nil {
		fmt.Println(errFull.Error())
	}

	assert.Nil(errFull)
	assert.NotNil(resultFull)

	// Indirect dereferences the pointer for us
	valOfRslt := reflect.Indirect(reflect.ValueOf(resultFull))
	typeOfRslt := valOfRslt.Type()

	var zerosFound []string

	for i := 0; i < typeOfRslt.NumField(); i++ {
		if valOfRslt.Field(i).IsZero() {
			zerosFound = append(zerosFound, typeOfRslt.Field(i).Name)
		}
	}

	// Check for empty fields in example config
	assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config %v - for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", hclFilenameFull, typeOfRslt, zerosFound))

	// Check the config values that are outside the statsreceiver part
	assert.NotZero(cfgFull.Data.StatsReceiver.BufferSec)
	assert.NotZero(cfgFull.Data.StatsReceiver.TimeoutSec)

	hclFilenameMinimal := filepath.Join("configs", "monitoring", "minimal", "statsd-minimal.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilenameMinimal)

	cfgMin, err := config.NewConfig()
	assert.NotNil(cfgMin)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	// Check the statsD part
	useMin := cfgMin.Data.StatsReceiver.Receiver
	decoderOptsMin := &config.DecoderOptions{
		Input: useMin.Body,
	}

	resultMin, errMin := cfgMin.CreateComponent(MockAdaptStatsDStatsReceiver(), decoderOptsMin)
	if errMin != nil {
		fmt.Println(errMin.Error())
	}

	assert.Nil(errMin)
	assert.NotNil(resultMin)

	// Check the config values that are outside the statsreceiver part
	assert.NotZero(cfgMin.Data.StatsReceiver.BufferSec)
	assert.NotZero(cfgMin.Data.StatsReceiver.TimeoutSec)

}

// TestSentryConfigs tests the sentry config examples by running the Init function.
// Since there's no convenient way to remove defaults, and the config is small, we don't mock anything.
func TestSentryConfigs(t *testing.T) {
	assert := assert.New(t)

	hclFilenameFull := filepath.Join("configs", "monitoring", "full", "sentry-full.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilenameFull)

	cfgFull, sentryEnabledFull, errFull := cmd.Init()

	assert.NotNil(cfgFull)
	assert.True(sentryEnabledFull)
	assert.Nil(errFull)

	valOfSentryCfg := reflect.Indirect(reflect.ValueOf(cfgFull.Data.Sentry))
	typeOfSentryCfg := valOfSentryCfg.Type()

	var zerosFound []string

	for i := 0; i < valOfSentryCfg.NumField(); i++ {
		if valOfSentryCfg.Field(i).IsZero() {
			zerosFound = append(zerosFound, typeOfSentryCfg.Field(i).Name)
		}
	}

	// Check for empty fields in example config
	assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config %v - for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", hclFilenameFull, typeOfSentryCfg, zerosFound))

	hclFilenameMin := filepath.Join("configs", "monitoring", "minimal", "sentry-minimal.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilenameMin)

	cfgMin, sentryEnabledMin, errMin := cmd.Init()

	assert.NotNil(cfgMin)
	assert.True(sentryEnabledMin)
	assert.Nil(errMin)
}
