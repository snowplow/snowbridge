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

// Base64DecodeConfig is a configuration object for the base64Decode transformation
type Base64DecodeConfig struct {
}

type base64DecodeAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f base64DecodeAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f base64DecodeAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &Base64DecodeConfig{}

	return cfg, nil
}

// base64DecodeAdapterGenerator returns a base64Decode transformation adapter.
func base64DecodeAdapterGenerator(f func(c *Base64DecodeConfig) (TransformationFunction, error)) base64DecodeAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Base64DecodeConfig)
		if !ok {
			return nil, errors.New("invalid input, expected Base64DecodeConfig")
		}

		return f(cfg)
	}
}

// base64DecodeConfigFunction returns an Base64Decode transformation function, from an Base64DecodeConfig.
func base64DecodeConfigFunction(c *Base64DecodeConfig) (TransformationFunction, error) {
	return Base64Decode, nil
}

// Base64DecodeConfigPair is a configuration pair for the Base64Decode transformation
var Base64DecodeConfigPair = config.ConfigurationPair{
	Name:   "base64Decode",
	Handle: base64DecodeAdapterGenerator(base64DecodeConfigFunction),
}

// Base64Decode is a specific transformation implementation to transform good enriched data within a message to Json
func Base64Decode(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {

	b64DecodedData := make([]byte, base64.StdEncoding.DecodedLen(len(message.Data)))
	_, err := base64.StdEncoding.Decode(b64DecodedData, message.Data)
	if err != nil {
		message.SetError(err)
		return nil, nil, message, nil
	}

	message.Data = b64DecodedData
	return message, nil, nil, nil
}
