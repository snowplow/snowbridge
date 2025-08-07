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

package sourceconfig

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
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

type adapter func(i any) (any, error)

func adapterGenerator(_ func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i any) (any, error) {
		return mockSource{}, nil
	}
}

func (f adapter) Create(i any) (any, error) {
	return f(i)
}

func (f adapter) ProvideDefault() (any, error) {
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

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}
	c.Data.Source.Use.Name = "mock"

	supportedSources := []config.ConfigurationPair{mockConfigPair}

	source, err := GetSource(c, supportedSources)

	assert.Equal(mockSource{}, source)
	assert.Nil(err)
}

// TestGetSource_InvalidSource tests that we throw an error when given an invalid source configuration
func TestGetSource_InvalidSource(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}
	c.Data.Source.Use.Name = "fake"

	supportedSources := []config.ConfigurationPair{}

	source, err := GetSource(c, supportedSources)
	assert.Nil(source)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("invalid source found: fake. Supported sources in this build: ", err.Error())
	}
}

// Mock a broken adapter generator implementation
func brokenAdapterGenerator(_ func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i any) (any, error) {
		return nil, nil
	}
}

var mockUnhappyConfigPair = config.ConfigurationPair{
	Name:   "mock",
	Handle: brokenAdapterGenerator(configfunction),
}

// TestGetSource_BadConfig tests the case where the configuration implementation is broken
func TestGetSource_BadConfig(t *testing.T) {
	assert := assert.New(t)

	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}
	c.Data.Source.Use.Name = "mock"

	supportedSources := []config.ConfigurationPair{mockUnhappyConfigPair}

	source, err := GetSource(c, supportedSources)

	assert.Nil(source)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("could not interpret source configuration for \"mock\"", err.Error())
	}
}
