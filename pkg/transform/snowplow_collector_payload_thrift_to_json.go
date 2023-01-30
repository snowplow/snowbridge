//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"context"
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"

	collectorpayload "github.com/snowplow/snowbridge/third_party/snowplow/collectorpayload"
)

// CollectorPayloadThriftToJSONConfig is a configuration object for the spCollectorPayloadThriftToJSON transformation
type CollectorPayloadThriftToJSONConfig struct {
}

type collectorPayloadThriftToJSONAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f collectorPayloadThriftToJSONAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f collectorPayloadThriftToJSONAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &CollectorPayloadThriftToJSONConfig{}

	return cfg, nil
}

// adapterGenerator returns a spCollectorPayloadThriftToJSON transformation adapter.
func collectorPayloadThriftToJSONAdapterGenerator(f func(c *CollectorPayloadThriftToJSONConfig) (TransformationFunction, error)) collectorPayloadThriftToJSONAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*CollectorPayloadThriftToJSONConfig)
		if !ok {
			return nil, errors.New("invalid input, expected collectorPayloadThriftToJSONConfig")
		}

		return f(cfg)
	}
}

// collectorPayloadThriftToJSONConfigFunction returns an spCollectorPayloadThriftToJSON transformation function, from an collectorPayloadThriftToJSONConfig.
func collectorPayloadThriftToJSONConfigFunction(c *CollectorPayloadThriftToJSONConfig) (TransformationFunction, error) {
	return SpCollectorPayloadThriftToJSON, nil
}

// CollectorPayloadThriftToJSONConfigPair is a configuration pair for the spCollectorPayloadThriftToJSON transformation
var CollectorPayloadThriftToJSONConfigPair = config.ConfigurationPair{
	Name:   "spCollectorPayloadThriftToJSON",
	Handle: collectorPayloadThriftToJSONAdapterGenerator(collectorPayloadThriftToJSONConfigFunction),
}

// SpCollectorPayloadThriftToJSON is a specific transformation implementation to transform a Thrift encoded Collector Payload
// to a JSON string representation.
func SpCollectorPayloadThriftToJSON(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
	ctx := context.Background()

	// Deserialize the Collector Payload to a struct
	res, deserializeErr := collectorpayload.BinaryDeserializer(ctx, message.Data)
	if deserializeErr != nil {
		message.SetError(deserializeErr)
		return nil, nil, message, nil
	}

	// Re-encode as a JSON string to be able to leverage it downstream
	resJSON, jsonErr := collectorpayload.ToJSON(res)
	if jsonErr != nil {
		message.SetError(jsonErr)
		return nil, nil, message, nil
	}

	message.Data = resJSON
	return message, nil, nil, intermediateState
}
