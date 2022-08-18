// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"errors"
	"os"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/target"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

// Tests of full example files require that we don't provide defaults, so we mock that method for anything that does so.
type MockHTTPTargetAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockHTTPTargetAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockHTTPTargetAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &target.HTTPTargetConfig{}

	return cfg, nil
}

func HTTPTargetAdapterNoDefaults() MockHTTPTargetAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.HTTPTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected HTTPTargetConfig")
		}

		return cfg, nil
	}

}

// Tests of full example files require that we don't provide defaults, so we mock that method for anything that does so.
type MockPubSubTargetAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockPubSubTargetAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockPubSubTargetAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &target.PubSubTargetConfig{}

	return cfg, nil
}

func PubSubTargetAdapterNoDefaults() MockPubSubTargetAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.PubSubTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected PubSubTargetConfig")
		}

		return cfg, nil
	}

}

// Tests of full example files require that we don't provide defaults, so we mock that method for anything that does so.
type MockEventHubTargetAdapterNoDefaults func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f MockEventHubTargetAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockEventHubTargetAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &target.EventHubConfig{}

	return cfg, nil
}

func EventHubTargetAdapterNoDefaults() MockEventHubTargetAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.EventHubConfig)
		if !ok {
			return nil, errors.New("invalid input, expected EventHubTargetConfig")
		}

		return cfg, nil
	}

}

type MockKafkaTargetAdapterNoDefaults func(i interface{}) (interface{}, error)

func (f MockKafkaTargetAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockKafkaTargetAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &target.KafkaConfig{}

	return cfg, nil
}

func KafkaTargetAdapterNoDefaults() MockKafkaTargetAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.KafkaConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KafkaTargetConfig")
		}

		return cfg, nil
	}

}

type MockKinesisTargetAdapterNoDefaults func(i interface{}) (interface{}, error)

func (f MockKinesisTargetAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockKinesisTargetAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &target.KinesisTargetConfig{}

	return cfg, nil
}

func KinesisTargetAdapterNoDefaults() MockKinesisTargetAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.KinesisTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KinesisTargetConfig")
		}

		return cfg, nil
	}

}

type MockSQSTargetAdapterNoDefaults func(i interface{}) (interface{}, error)

func (f MockSQSTargetAdapterNoDefaults) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f MockSQSTargetAdapterNoDefaults) ProvideDefault() (interface{}, error) {
	// Don't provide any defaults
	cfg := &target.SQSTargetConfig{}

	return cfg, nil
}

func SQSTargetAdapterNoDefaults() MockSQSTargetAdapterNoDefaults {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.SQSTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected SQSTargetConfig")
		}

		return cfg, nil
	}

}
