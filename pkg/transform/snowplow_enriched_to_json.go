// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"errors"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// We could avoid all the config-related trimmings for this one, but providing them means that this
// transformation's validation is handled with all the same logic as the others, so it's safer.

// enrichedToJSONConfig is a configuration object for the spEnrichedToJson transformation
type enrichedToJSONConfig struct {
}

type enrichedToJSONAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f enrichedToJSONAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f enrichedToJSONAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &enrichedToJSONConfig{}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedToJson transformation adapter.
func enrichedToJSONAdapterGenerator(f func(c *enrichedToJSONConfig) (TransformationFunction, error)) enrichedToJSONAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*enrichedToJSONConfig)
		if !ok {
			return nil, errors.New("invalid input, expected enrichedToJSONConfig")
		}

		return f(cfg)
	}
}

// enrichedToJSONConfigFunction returns an spEnrichedToJson transformation function, from an enrichedToJSONConfig.
func enrichedToJSONConfigFunction(c *enrichedToJSONConfig) (TransformationFunction, error) {
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
