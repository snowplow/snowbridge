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

	"github.com/snowplow-devops/stream-replicator/config"
	kinesissource "github.com/snowplow-devops/stream-replicator/pkg/source/kinesis"
	pubsubsource "github.com/snowplow-devops/stream-replicator/pkg/source/pubsub"
	sqssource "github.com/snowplow-devops/stream-replicator/pkg/source/sqs"
	stdinsource "github.com/snowplow-devops/stream-replicator/pkg/source/stdin"
	"github.com/stretchr/testify/assert"
)

func TestSourceDocumentation(t *testing.T) {
	// Set env vars referenced in the config examples
	t.Setenv("MY_AUTH_PASSWORD", "test")
	t.Setenv("SASL_PASSWORD", "test")

	sourcesToTest := []string{"kinesis", "pubsub", "sqs", "stdin"}

	for _, src := range sourcesToTest {

		// Read file:
		minimalFilePath := filepath.Join("documentation-examples", "configuration", "sources", src+"-minimal-example.hcl")
		fullFilePath := filepath.Join("documentation-examples", "configuration", "sources", src+"-full-example.hcl")

		// Test minimal config
		testMinimalSourceConfig(t, minimalFilePath)
		// Test full config
		testFullSourceConfig(t, fullFilePath)
	}
}

func testMinimalSourceConfig(t *testing.T, filepath string) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.Source.Use
	decoderOpts := &config.DecoderOptions{
		Input: use.Body,
	}

	var result interface{}
	var err error

	switch use.Name {
	case "kinesis":
		result, err = c.CreateComponent(mockKinesisSourceAdapter(), decoderOpts)
	case "pubsub":
		result, err = c.CreateComponent(mockPubSubSourceAdapter(), decoderOpts)
	case "sqs":
		result, err = c.CreateComponent(mockSQSSourceAdapter(), decoderOpts)
	case "stdin":
		result, err = c.CreateComponent(mockStdinSourceAdapter(), decoderOpts)
	default:
		result, err = nil, errors.New(fmt.Sprint("Source not recognised: ", use.Name))
	}

	assert.NotNil(result)
	if err != nil {
		assert.Fail(use.Name, err.Error())
	}
	assert.Nil(err)
}

func testFullSourceConfig(t *testing.T, filepath string) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, filepath)

	use := c.Data.Source.Use
	decoderOpts := &config.DecoderOptions{
		Input: use.Body,
	}

	var result interface{}
	var err error

	switch use.Name {
	case "kinesis":
		result, err = c.CreateComponent(mockKinesisSourceAdapterNoDefaults(), decoderOpts)
	case "pubsub":
		result, err = c.CreateComponent(mockPubSubSourceAdapterNoDefaults(), decoderOpts)
	case "sqs":
		result, err = c.CreateComponent(mockSQSSourceAdapterNoDefaults(), decoderOpts)
	case "stdin":
		result, err = c.CreateComponent(mockStdinSourceAdapterNoDefaults(), decoderOpts)
	default:
		result, err = nil, errors.New(fmt.Sprint("Source not recognised: ", use.Name))
	}

	if err != nil {
		assert.Fail(use.Name, err.Error())
	}
	assert.Nil(err)
	assert.NotNil(result)

	// Skip the next bit if we failed the above - it will panic if result doesn't exist, making the debug annoying
	if result == nil {
		return
	}

	// TODO: We can replace this with checkComponentForZeros.

	// Indirect dereferences the pointer for us
	valOfRslt := reflect.Indirect(reflect.ValueOf(result))
	typeOfRslt := valOfRslt.Type()

	var zerosFound []string

	for i := 0; i < typeOfRslt.NumField(); i++ {
		if valOfRslt.Field(i).IsZero() {
			zerosFound = append(zerosFound, typeOfRslt.Field(i).Name)
		}
	}

	// Check for empty fields in example config
	assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", typeOfRslt, zerosFound))
}

// Tests of full example files require that we don't provide defaults, so we mock that method for anything that does so.

// Kinesis

type MockKinesisSourceAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockKinesisSourceAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockKinesisSourceAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &kinesissource.Configuration{}

	return cfg, nil
}

func mockKinesisSourceAdapterNoDefaults() MockKinesisSourceAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*kinesissource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected kinesissource.Configuration")
		}

		return cfg, nil
	}

}

func mockKinesisSourceAdapter() kinesissource.Adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*kinesissource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected configuration for kinesis source")
		}

		return cfg, nil
	}
}

// Pubsub

type MockPubSubSourceAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockPubSubSourceAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockPubSubSourceAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &pubsubsource.Configuration{}

	return cfg, nil
}

func mockPubSubSourceAdapterNoDefaults() MockPubSubSourceAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*pubsubsource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected pubsubsource.Configuration")
		}

		return cfg, nil
	}

}

func mockPubSubSourceAdapter() pubsubsource.Adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*pubsubsource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected configuration for pubsub source")
		}

		return cfg, nil
	}
}

func mockPubSubSourceFunc(c *pubsubsource.Configuration) (*pubsubsource.Configuration, error) {
	return c, nil
}

// SQS

// Tests of full example files require that we don't provide defaults, so we mock that method for anything that does so.
type MockSQSSourceAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockSQSSourceAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockSQSSourceAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &sqssource.Configuration{}

	return cfg, nil
}

func mockSQSSourceAdapterNoDefaults() MockSQSSourceAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*sqssource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected sqssource.Configuration")
		}

		return cfg, nil
	}

}

func mockSQSSourceAdapter() sqssource.Adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*sqssource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected configuration for SQS source")
		}

		return cfg, nil
	}
}

// Stdin

// Tests of full example files require that we don't provide defaults, so we mock that method for anything that does so.
type MockStdinSourceAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockStdinSourceAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockStdinSourceAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &stdinsource.Configuration{}

	return cfg, nil
}

func mockStdinSourceAdapterNoDefaults() MockStdinSourceAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*stdinsource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected stdinsource.Configuration")
		}

		return cfg, nil
	}

}

func mockStdinSourceAdapter() stdinsource.Adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*stdinsource.Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected configuration for SQS source")
		}

		return cfg, nil
	}
}
