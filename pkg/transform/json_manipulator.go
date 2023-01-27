//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	// "context"
	// "encoding/base64"
	"encoding/json"
	"errors"
	"time"
	"fmt"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// JSONManipulatorConfig is a configuration object for the JSONManipulator transformation
type JSONManipulatorConfig struct {
	KeyRename    map[string]string `hcl:"key_rename"`
	KeyValueFunc map[string]string `hcl:"key_value_func"`
}

// JSONManipulatorAdapter is a configuration object for the JSONManipulator transformation
type JSONManipulatorAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f JSONManipulatorAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f JSONManipulatorAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &JSONManipulatorConfig{
		KeyRename:    make(map[string]string),
		KeyValueFunc: make(map[string]string),
	}

	return cfg, nil
}

// JSONManipulatorAdapterGenerator returns a JSONManipulator transformation adapter.
func JSONManipulatorAdapterGenerator(f func(c *JSONManipulatorConfig) (TransformationFunction, error)) JSONManipulatorAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*JSONManipulatorConfig)
		if !ok {
			return nil, errors.New("invalid input, expected JSONManipulatorConfig")
		}

		return f(cfg)
	}
}

// JSONManipulatorConfigFunction returns an JSONManipulator transformation function, from an JSONManipulatorConfig.
func JSONManipulatorConfigFunction(c *JSONManipulatorConfig) (TransformationFunction, error) {
	return NewJSONManipulator(
		c.KeyRename,
		c.KeyValueFunc,
	)
}

// JSONManipulatorConfigPair is a configuration pair for the JSONManipulator transformation
var JSONManipulatorConfigPair = config.ConfigurationPair{
	Name:   "JSONManipulator",
	Handle: JSONManipulatorAdapterGenerator(JSONManipulatorConfigFunction),
}

// --- Manipulator Value Functions

// timeToEpochMillis attempts to convert an RFC3339 string to a Unix Timestamp in milliseconds
func timeToEpochMillis(v interface{}) (int64, error) {
	switch v.(type) {
	case string:
		vTime, err := time.Parse(time.RFC3339, v.(string))
		if err != nil {
			return -1, err
		}
		return vTime.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond)), nil
	default:
		return -1, errors.New(fmt.Sprintf("input value for 'timeToEpochMillis' must be a 'string' was '%T'", v))
	}
}

// --- Manipulator Functions

// mapKeyRename takes an input map and renames keys it finds in the replace instructions
func mapKeyRename(input map[string]interface{}, keyRename map[string]string) map[string]interface{} {
	for old, new := range keyRename {
		if _, ok := input[old]; ok {
			input[new] = input[old]
			delete(input, old)
		}
	}
	return input
}

// mapKeyValueFunc runs pre-defined functions against a value specified by the input key
func mapKeyValueFunc(input map[string]interface{}, keyValueFunc map[string]string) (map[string]interface{}, error) {
	for key, funcToRun := range keyValueFunc {
		if val, ok := input[key]; ok {
			switch funcToRun {
			case "timeToEpochMillis":
				valTime, err := timeToEpochMillis(val)
				if err != nil {
					return nil, err
				}
				input[key] = valTime
			default:
				input[key] = val
			}
		}
	}
	return input, nil
}

// NewJSONManipulator returns a transformation implementation to transform an input JSON string according to the configured manipulation
// instructions provided in the configuration
func NewJSONManipulator(keyRename map[string]string, keyValueFunc map[string]string) (TransformationFunction, error) {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// Unmarshal inbound message to a map
		var input map[string]interface{}
		unmarshallErr := json.Unmarshal(message.Data, &input)
		if unmarshallErr != nil {
			message.SetError(unmarshallErr)
			return nil, nil, message, nil
		}

		// 1. Rename keys in input JSON
		renamed := mapKeyRename(input, keyRename)

		// 2. Apply value functions on renamed JSON
		manipulated, valueFuncErr := mapKeyValueFunc(renamed, keyValueFunc)
		if valueFuncErr != nil {
			message.SetError(valueFuncErr)
			return nil, nil, message, nil
		}

		// Marshal back to a JSON string
		res, jsonErr := json.Marshal(manipulated)
		if jsonErr != nil {
			message.SetError(jsonErr)
			return nil, nil, message, nil
		}
		message.Data = res
		return message, nil, nil, intermediateState
	}, nil
}
