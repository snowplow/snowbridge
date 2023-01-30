//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"

	collectorpayload "github.com/snowplow/snowbridge/third_party/snowplow/collectorpayload"
	collectorpayloadmodel1 "github.com/snowplow/snowbridge/third_party/snowplow/collectorpayload/gen-go/model1"
)

// JSONToCollectorPayloadThriftConfig is a configuration object for the spJSONToCollectorPayloadThrift transformation
type JSONToCollectorPayloadThriftConfig struct {
	Base64Encode bool `hcl:"base_64_encode"`
}

// JSONToCollectorPayloadThriftAdapter is a configuration object for the spJSONToCollectorPayloadThrift transformation
type JSONToCollectorPayloadThriftAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f JSONToCollectorPayloadThriftAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f JSONToCollectorPayloadThriftAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &JSONToCollectorPayloadThriftConfig{
		Base64Encode: false,
	}

	return cfg, nil
}

// JSONToCollectorPayloadThriftAdapterGenerator returns a spJSONToCollectorPayloadThrift transformation adapter.
func JSONToCollectorPayloadThriftAdapterGenerator(f func(c *JSONToCollectorPayloadThriftConfig) (TransformationFunction, error)) JSONToCollectorPayloadThriftAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*JSONToCollectorPayloadThriftConfig)
		if !ok {
			return nil, errors.New("invalid input, expected JSONToCollectorPayloadThriftConfig")
		}

		return f(cfg)
	}
}

// JSONToCollectorPayloadThriftConfigFunction returns an spJSONToCollectorPayloadThrift transformation function, from an JSONToCollectorPayloadThriftConfig.
func JSONToCollectorPayloadThriftConfigFunction(c *JSONToCollectorPayloadThriftConfig) (TransformationFunction, error) {
	return NewSpJSONToCollectorPayloadThrift(
		c.Base64Encode,
	)
}

// JSONToCollectorPayloadThriftConfigPair is a configuration pair for the spJSONToCollectorPayloadThrift transformation
var JSONToCollectorPayloadThriftConfigPair = config.ConfigurationPair{
	Name:   "spJSONToCollectorPayloadThrift",
	Handle: JSONToCollectorPayloadThriftAdapterGenerator(JSONToCollectorPayloadThriftConfigFunction),
}

// NewSpJSONToCollectorPayloadThrift returns a transformation implementation to transform a raw message into a valid Thrift encoded Collector Payload
// so that it can be pushed directly into the egress stream of a Collector.
func NewSpJSONToCollectorPayloadThrift(base64Encode bool) (TransformationFunction, error) {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		var p *collectorpayloadmodel1.CollectorPayload
		unmarshallErr := json.Unmarshal(message.Data, &p)
		if unmarshallErr != nil {
			message.SetError(unmarshallErr)
			return nil, nil, message, nil
		}

		ctx := context.Background()

		res, serializeErr := collectorpayload.BinarySerializer(ctx, p)
		if serializeErr != nil {
			message.SetError(serializeErr)
			return nil, nil, message, nil
		}

		// Optionally base64 encode the output
		if base64Encode {
			message.Data = []byte(base64.StdEncoding.EncodeToString(res))
		} else {
			message.Data = res
		}

		return message, nil, nil, intermediateState
	}, nil
}
