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

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/config"
	kinesissource "github.com/snowplow-devops/stream-replicator/pkg/source/kinesis"
	pubsubsource "github.com/snowplow-devops/stream-replicator/pkg/source/pubsub"
	sqssource "github.com/snowplow-devops/stream-replicator/pkg/source/sqs"
	stdinsource "github.com/snowplow-devops/stream-replicator/pkg/source/stdin"
)

// TODO: This passes when we remove a case from targetsToTest. Is there an approach which fails if we miss a case?

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

// TestSourceDocsFullConfigs tests config examples who have all options fully populated.
// We use mocks to prevent default values from being set.
// This test will fail if we either don't have an example config file for a target, or we do have one but not all values are set.
// Because we can't tell between a missing and a zero value, zero values in the examples will cause a false negative.
func TestSourceDocsFullConfigs(t *testing.T) {
	assert := assert.New(t)

	// Because we use `env` in examples, we need this to be set.
	t.Setenv("MY_AUTH_PASSWORD", "test")

	sourcesToTest := []string{"kinesis", "pubsub", "sqs", "stdin"}

	for _, source := range sourcesToTest {
		hclFilename := filepath.Join("configs", "source", "full", fmt.Sprintf("%s-full.hcl", source))
		t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilename)

		c, err := config.NewConfig()
		assert.NotNil(c)
		if err != nil {
			t.Fatalf("function NewConfig failed with error: %q", err.Error())
		}

		use := c.Data.Source.Use
		decoderOpts := &config.DecoderOptions{
			Input: use.Body,
		}

		var result interface{}

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
			assert.Fail(fmt.Sprintf("Source not recognised: %v", source))
		}

		assert.Nil(err)
		assert.NotNil(result)

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
		assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config %v - for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", hclFilename, typeOfRslt, zerosFound))
	}
}

// TestSourceDocsMinimalConfigs tests minimal config examples.
func TestSourceDocsMinimalConfigs(t *testing.T) {
	assert := assert.New(t)

	// Because we use `env` in examples, we need this to be set.
	t.Setenv("MY_AUTH_PASSWORD", "test")

	sourcesToTest := []string{"kinesis", "pubsub", "sqs", "stdin"}

	for _, source := range sourcesToTest {
		hclFilename := filepath.Join("configs", "source", "minimal", fmt.Sprintf("%s-minimal.hcl", source))
		t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilename)

		c, err := config.NewConfig()
		assert.NotNil(c)
		if err != nil {
			t.Fatalf("function NewConfig failed with error: %q", err.Error())
		}

		use := c.Data.Source.Use
		decoderOpts := &config.DecoderOptions{
			Input: use.Body,
		}

		var result interface{}

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
			assert.Fail(fmt.Sprintf("Source not recognised: %v", source))
		}

		assert.Nil(err)
		assert.NotNil(result)
	}
}
