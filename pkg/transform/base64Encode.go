//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"encoding/base64"
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// We could avoid all the config-related trimmings for this one, but providing them means that this
// transformation's validation is handled with all the same logic as the others, so it's safer.

// Base64EncodeConfig is a configuration object for the base64Encode transformation
type Base64EncodeConfig struct {
}

type base64EncodeAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f base64EncodeAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f base64EncodeAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &Base64EncodeConfig{}

	return cfg, nil
}

// base64EncodeAdapterGenerator returns a base64Encode transformation adapter.
func base64EncodeAdapterGenerator(f func(c *Base64EncodeConfig) (TransformationFunction, error)) base64EncodeAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Base64EncodeConfig)
		if !ok {
			return nil, errors.New("invalid input, expected Base64EncodeConfig")
		}

		return f(cfg)
	}
}

// base64EncodeConfigFunction returns an Base64Encode transformation function, from an Base64EncodeConfig.
func base64EncodeConfigFunction(c *Base64EncodeConfig) (TransformationFunction, error) {
	return Base64Encode, nil
}

// Base64EncodeConfigPair is a configuration pair for the Base64Encode transformation
var Base64EncodeConfigPair = config.ConfigurationPair{
	Name:   "base64Encode",
	Handle: base64EncodeAdapterGenerator(base64EncodeConfigFunction),
}

// Base64Encode is a specific transformation implementation to transform good enriched data within a message to Json
func Base64Encode(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {

	b64EncodedData := make([]byte, base64.StdEncoding.EncodedLen(len(message.Data)))
	base64.StdEncoding.Encode(b64EncodedData, message.Data)
	// Encode doesn't return anything

	message.Data = b64EncodedData
	return message, nil, nil, nil
}
