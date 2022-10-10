// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"errors"
	"fmt"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// setPkConfig is a configuration object for the spEnrichedSetPk transformation
type setPkConfig struct {
	AtomicField string `hcl:"atomic_field"`
}

// The adapter type is an adapter for functions to be used as
// pluggable components for spEnrichedSetPk transformation. It implements the Pluggable interface.
type setPkAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f setPkAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f setPkAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &setPkConfig{}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedSetPk transformation adapter.
func setPkAdapterGenerator(f func(c *setPkConfig) (TransformationFunction, error)) setPkAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*setPkConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// setPkConfigFunction returns an spEnrichedSetPk transformation function, from an setPkConfig.
func setPkConfigFunction(c *setPkConfig) (TransformationFunction, error) {
	return NewSpEnrichedSetPkFunction(
		c.AtomicField,
	)
}

// SetPkConfigPair is a configuration pair for the spEnrichedSetPk transformation
var SetPkConfigPair = config.ConfigurationPair{
	Name:   "spEnrichedSetPk",
	Handle: setPkAdapterGenerator(setPkConfigFunction),
}

// TODO: This function should check if the field provided is a valid atomic field, and throw an error if not.

// NewSpEnrichedSetPkFunction returns a TransformationFunction which sets the partition key of a message to a field within a Snowplow enriched event
func NewSpEnrichedSetPkFunction(pkField string) (TransformationFunction, error) {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// Evalute intermediateState to parsedEvent
		parsedEvent, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		pk, err := parsedEvent.GetValue(pkField)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}
		message.PartitionKey = fmt.Sprintf("%v", pk)
		return message, nil, nil, parsedEvent
	}, nil
}
