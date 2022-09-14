// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/statsreceiver"
	"github.com/stretchr/testify/assert"
)

func TestMonitoringDocumentation(t *testing.T) {
	assert := assert.New(t)

	markdownFilePath := filepath.Join("documentation", "configuration", "monitoring", "monitoring.md")

	fencedBlocksFound := getFencedHCLBlocksFromMd(markdownFilePath)

	// Since all code blocks are separate here we can just join them into one
	codeBlock := strings.Join(fencedBlocksFound, "\n")

	c := createConfigFromCodeBlock(t, codeBlock)

	use := c.Data.StatsReceiver.Receiver
	decoderOptsFull := &config.DecoderOptions{
		Input: use.Body,
	}

	statsd, err := c.CreateComponent(MockAdaptStatsDStatsReceiverNoDefaults(), decoderOptsFull)
	if err != nil {
		fmt.Println(err.Error())
	}

	assert.Nil(err)
	assert.NotNil(statsd)

	checkComponentForZeros(t, statsd)

	// Check the config values that are outside the statsreceiver part
	assert.NotZero(c.Data.StatsReceiver.BufferSec)
	assert.NotZero(c.Data.StatsReceiver.TimeoutSec)

	// Check that loglevel isn't the default.
	assert.NotEqual("info", c.Data.LogLevel)

	// Repeating some code from  createConfigFromCodeBlock here so we can call init.
	// We can prob factor better.

	tmpConfigPath := filepath.Join("tmp", "config.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", tmpConfigPath)

	configFile, err := os.Create(tmpConfigPath)
	if err != nil {
		panic(err)
	}

	defer configFile.Close()

	// Write shortest one to minimal
	_, err2 := configFile.WriteString(codeBlock)
	if err != nil {
		assert.Fail(err.Error())
		panic(err2)
	}
	// Since sentry lives in cmd, we call Init to test it.
	cfgSentry, sentryEnabled, initErr := cmd.Init()

	assert.NotNil(cfgSentry)
	assert.True(sentryEnabled)
	assert.Nil(initErr)

	checkComponentForZeros(t, cfgSentry.Data.Sentry)

}

func checkComponentForZeros(t *testing.T, component interface{}) {
	assert := assert.New(t)

	// Indirect dereferences the pointer for us
	valOfComponent := reflect.Indirect(reflect.ValueOf(component))
	typeOfComponent := valOfComponent.Type()

	var zerosFound []string

	for i := 0; i < typeOfComponent.NumField(); i++ {
		if valOfComponent.Field(i).IsZero() {
			zerosFound = append(zerosFound, typeOfComponent.Field(i).Name)
		}
	}

	// Check for empty fields in example config
	assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", typeOfComponent, zerosFound))
}

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
