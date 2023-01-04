//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"errors"
	"fmt"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// SetPkConfig is a configuration object for the spEnrichedSetPk transformation
type SetPkConfig struct {
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
	cfg := &SetPkConfig{}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedSetPk transformation adapter.
func setPkAdapterGenerator(f func(c *SetPkConfig) (TransformationFunction, error)) setPkAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*SetPkConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// setPkConfigFunction returns an spEnrichedSetPk transformation function, from an setPkConfig.
func setPkConfigFunction(c *SetPkConfig) (TransformationFunction, error) {
	return NewSpEnrichedSetPkFunction(
		c.AtomicField,
	)
}

// SetPkConfigPair is a configuration pair for the spEnrichedSetPk transformation
var SetPkConfigPair = config.ConfigurationPair{
	Name:   "spEnrichedSetPk",
	Handle: setPkAdapterGenerator(setPkConfigFunction),
}

// NewSpEnrichedSetPkFunction returns a TransformationFunction which sets the partition key of a message to a field within a Snowplow enriched event
func NewSpEnrichedSetPkFunction(pkField string) (TransformationFunction, error) {

	// Validate the field provided
	err := ValidateAtomicField(pkField)
	if err != nil {
		return nil, err
	}

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
