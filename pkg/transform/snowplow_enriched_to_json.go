//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// We could avoid all the config-related trimmings for this one, but providing them means that this
// transformation's validation is handled with all the same logic as the others, so it's safer.

// EnrichedToJSONConfig is a configuration object for the spEnrichedToJson transformation
type EnrichedToJSONConfig struct {
}

type enrichedToJSONAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f enrichedToJSONAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f enrichedToJSONAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &EnrichedToJSONConfig{}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedToJson transformation adapter.
func enrichedToJSONAdapterGenerator(f func(c *EnrichedToJSONConfig) (TransformationFunction, error)) enrichedToJSONAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*EnrichedToJSONConfig)
		if !ok {
			return nil, errors.New("invalid input, expected enrichedToJSONConfig")
		}

		return f(cfg)
	}
}

// enrichedToJSONConfigFunction returns an spEnrichedToJson transformation function, from an enrichedToJSONConfig.
func enrichedToJSONConfigFunction(c *EnrichedToJSONConfig) (TransformationFunction, error) {
	return SpEnrichedToJSON, nil
}

// EnrichedToJSONConfigPair is a configuration pair for the spEnrichedToJson transformation
var EnrichedToJSONConfigPair = config.ConfigurationPair{
	Name:   "spEnrichedToJson",
	Handle: enrichedToJSONAdapterGenerator(enrichedToJSONConfigFunction),
}

// SpEnrichedToJSON is a specific transformation implementation to transform good enriched data within a message to Json
func SpEnrichedToJSON(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
	// Evalute intermediateState to parsedEvent
	parsedEvent, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
	if parseErr != nil {
		message.SetError(parseErr)
		return nil, nil, message, nil
	}

	jsonMessage, err := parsedEvent.ToJson()
	if err != nil {
		message.SetError(err)
		return nil, nil, message, nil
	}
	message.Data = jsonMessage
	return message, nil, nil, parsedEvent
}
