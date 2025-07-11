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

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// We could avoid all the config-related trimmings for this one, but providing them means that this
// transformation's validation is handled with all the same logic as the others, so it's safer.

// EnrichedToJSONConfig is a configuration object for the spEnrichedToJson transformation
type EnrichedToJSONConfig struct {
}

type enrichedToJSONAdapter func(i any) (any, error)

// Create implements the ComponentCreator interface.
func (f enrichedToJSONAdapter) Create(i any) (any, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f enrichedToJSONAdapter) ProvideDefault() (any, error) {
	// Provide defaults
	cfg := &EnrichedToJSONConfig{}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedToJson transformation adapter.
func enrichedToJSONAdapterGenerator(f func(c *EnrichedToJSONConfig) (TransformationFunction, error)) enrichedToJSONAdapter {
	return func(i any) (any, error) {
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
func SpEnrichedToJSON(message *models.Message, intermediateState any) (*models.Message, *models.Message, *models.Message, any) {
	// Evalute intermediateState to parsedEvent
	parsedEvent, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
	if parseErr != nil {
		message.SetError(parseErr)
		message.SetErrorType(models.ErrorTypeTransformation)
		return nil, nil, message, nil
	}

	jsonMessage, err := parsedEvent.ToJson()
	if err != nil {
		message.SetError(err)
		message.SetErrorType(models.ErrorTypeTransformation)
		return nil, nil, message, nil
	}
	message.Data = jsonMessage
	return message, nil, nil, parsedEvent
}
