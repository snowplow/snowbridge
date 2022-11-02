// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package sourceconfig

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	config "github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

// Mock a Source and configuration
type mockSource struct{}

func (m mockSource) Read(sf *sourceiface.SourceFunctions) error {
	return nil
}

func (m mockSource) Stop() {}

func (m mockSource) GetID() string {
	return ""
}

type configuration struct{}

func configfunction(c *configuration) (sourceiface.Source, error) {
	return mockSource{}, nil
}

type adapter func(i interface{}) (interface{}, error)

func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		return mockSource{}, nil
	}
}

func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &configuration{}

	return cfg, nil
}

var mockConfigPair = config.ConfigurationPair{
	Name:   "mock",
	Handle: adapterGenerator(configfunction),
}

// TestGetSource_ValidSource tests the happy path for GetSource
func TestGetSource_ValidSource(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SOURCE_NAME", "mock")

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	supportedSources := []config.ConfigurationPair{mockConfigPair}

	source, err := GetSource(c, supportedSources)

	assert.Equal(mockSource{}, source)
	assert.Nil(err)
}

// TestGetSource_InvalidSource tests that we throw an error when given an invalid source configuration
func TestGetSource_InvalidSource(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SOURCE_NAME", "fake")

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	supportedSources := []config.ConfigurationPair{}

	source, err := GetSource(c, supportedSources)
	assert.Nil(source)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Invalid source found: fake. Supported sources in this build: ", err.Error())
	}
}

// Mock a broken adapter generator implementation
func brokenAdapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		return nil, nil
	}
}

var mockUnhappyConfigPair = config.ConfigurationPair{
	Name:   "mockUnhappy",
	Handle: brokenAdapterGenerator(configfunction),
}

// TestGetSource_BadConfig tests the case where the configuration implementation is broken
func TestGetSource_BadConfig(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SOURCE_NAME", "mockUnhappy")

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	supportedSources := []config.ConfigurationPair{mockUnhappyConfigPair}

	source, err := GetSource(c, supportedSources)

	assert.Nil(source)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("could not interpret source configuration for \"mockUnhappy\"", err.Error())
	}
}
