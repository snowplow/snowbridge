/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package transform

import (
	"encoding/json"
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// JQMapperConfig represents the configuration for the JQ transformation
type JQMapperConfig struct {
	JQCommand    string `hcl:"jq_command"`
	RunTimeoutMs int    `hcl:"timeout_ms,optional"`
	SpMode       bool   `hcl:"snowplow_mode,optional"`
}

// JQMapperConfigPair is a configuration pair for the jq mapper transformation
var JQMapperConfigPair = config.ConfigurationPair{
	Name:   "jq",
	Handle: jqMapperAdapterGenerator(jqMapperConfigFunction),
}

// jqMapperConfigFunction returns a jq mapper transformation function from a JQMapperConfig
func jqMapperConfigFunction(c *JQMapperConfig) (TransformationFunction, error) {
	return GojqTransformationFunction(c.JQCommand, c.RunTimeoutMs, c.SpMode, transformOutput)
}

func transformOutput(jqOutput JqCommandOutput) TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		removeNullFields(jqOutput)

		// here v is any, so we Marshal. alternative: gojq.Marshal
		data, err := json.Marshal(jqOutput)
		if err != nil {
			message.SetError(errors.New("error encoding jq query output data"))
			return nil, nil, message, nil
		}

		message.Data = data
		return message, nil, nil, nil
	}
}

func removeNullFields(data any) {
	switch input := data.(type) {
	case map[string]any:
		removeNullFromMap(input)
	case []any:
		removeNullFromSlice(input)
	default:
		return
	}
}

func removeNullFromMap(input map[string]any) {
	for key := range input {
		field := input[key]
		if field == nil {
			delete(input, key)
			continue
		}
		removeNullFields(field)
	}
}

func removeNullFromSlice(input []any) {
	for _, item := range input {
		removeNullFields(item)
	}
}

// jqMapperAdapterGenerator returns a jqAdapter
func jqMapperAdapterGenerator(f func(c *JQMapperConfig) (TransformationFunction, error)) jqMapperAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*JQMapperConfig)
		if !ok {
			return nil, errors.New("invalid input, expected JQMapperConfig")
		}

		return f(cfg)
	}
}

// jqMapperAdapter implements the Pluggable interface
type jqMapperAdapter func(i interface{}) (interface{}, error)

// ProvideDefault implements the ComponentConfigurable interface
func (f jqMapperAdapter) ProvideDefault() (interface{}, error) {
	return &JQMapperConfig{
		RunTimeoutMs: 100,
	}, nil
}

// Create implements the ComponentCreator interface
func (f jqMapperAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}
