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
type setPkAdapter func(i any) (any, error)

// Create implements the ComponentCreator interface.
func (f setPkAdapter) Create(i any) (any, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f setPkAdapter) ProvideDefault() (any, error) {
	// Provide defaults
	cfg := &SetPkConfig{}

	return cfg, nil
}

// setPkAdapterGenerator returns a spEnrichedSetPk transformation adapter.
func setPkAdapterGenerator(f func(c *SetPkConfig) (TransformationFunction, error)) setPkAdapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*SetPkConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedSetPKConfig")
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

	return func(message *models.Message, intermediateState any) (*models.Message, *models.Message, *models.Message, any) {
		// Evalute intermediateState to parsedEvent
		parsedEvent, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "intermediate state cannot be parsed as parsedEvent",
				Err:         parseErr,
			})
			return nil, nil, message, nil
		}

		pk, err := parsedEvent.GetValue(pkField)
		if err != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "failed to get value of the provided atomic field",
				Err:         err,
			})
			return nil, nil, message, nil
		}
		message.PartitionKey = fmt.Sprintf("%v", pk)
		return message, nil, nil, parsedEvent
	}, nil
}
