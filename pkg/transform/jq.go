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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/itchyny/gojq"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// JQMapperConfig represents the configuration for the JQ transformation
type JQMapperConfig struct {
	JQCommand    string `hcl:"jq_command"`
	RunTimeoutMs int    `hcl:"timeout_ms,optional"`
	SpMode       bool   `hcl:"snowplow_mode,optional"`
}

// JQMapper handles jq generic mapping as a transformation
type jqMapper struct {
	JQCode       *gojq.Code
	RunTimeoutMs time.Duration
	SpMode       bool
}

// RunFunction runs a jq mapper transformation
func (jqm *jqMapper) RunFunction() TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		input, err := mkJQInput(jqm, message, interState)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), jqm.RunTimeoutMs)
		defer cancel()

		iter := jqm.JQCode.RunWithContext(ctx, input)
		// no looping since we only keep first value
		v, ok := iter.Next()
		if !ok {
			message.SetError(errors.New("jq query got no output"))
			return nil, nil, message, nil
		}

		if err, ok := v.(error); ok {
			message.SetError(err)
			return nil, nil, message, nil
		}

		removeNullFields(v)

		// here v is any, so we Marshal. alternative: gojq.Marshal
		data, err := json.Marshal(v)
		if err != nil {
			message.SetError(errors.New("error encoding jq query output data"))
			return nil, nil, message, nil
		}

		message.Data = data
		return message, nil, nil, nil
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

// jqMapperConfigFunction returns a jq mapper transformation function from a JQMapperConfig
func jqMapperConfigFunction(c *JQMapperConfig) (TransformationFunction, error) {
	query, err := gojq.Parse(c.JQCommand)
	if err != nil {
		return nil, fmt.Errorf("error parsing jq command: %s", err)
	}

	// epoch converts a time.Time to an epoch in seconds, as integer type.
	// It must be an integer in order to chain with jq-native time functions
	withEpochFunction := gojq.WithFunction("epoch", 0, 1, func(a1 any, a2 []any) any {
		if a1 == nil {
			return nil
		}

		validTime, ok := a1.(time.Time)

		if !ok {
			return errors.New("Not a valid time input to 'epoch' function")
		}

		return int(validTime.Unix())
	})

	// epochMillis converts a time.Time to an epoch in milliseconds
	withEpochMillisFunction := gojq.WithFunction("epochMillis", 0, 1, func(a1 any, a2 []any) any {
		if a1 == nil {
			return nil
		}

		validTime, ok := a1.(time.Time)

		if !ok {
			return errors.New("Not a valid time input to 'epochMillis' function")
		}

		return validTime.UnixMilli()
	})

	code, err := gojq.Compile(query, withEpochMillisFunction, withEpochFunction)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %s", err)
	}

	jq := &jqMapper{
		JQCode:       code,
		RunTimeoutMs: time.Duration(c.RunTimeoutMs) * time.Millisecond,
		SpMode:       c.SpMode,
	}

	return jq.RunFunction(), nil
}

// JQMapperConfigPair is a configuration pair for the jq mapper transformation
var JQMapperConfigPair = config.ConfigurationPair{
	Name:   "jq",
	Handle: jqMapperAdapterGenerator(jqMapperConfigFunction),
}

// mkJQInput ensures the input to JQ query is of expected type
func mkJQInput(jqm *jqMapper, message *models.Message, interState interface{}) (map[string]interface{}, error) {
	if !jqm.SpMode {
		// gojq input can only be map[string]any or []any
		// here we only consider the first, but we could also expand
		var input map[string]interface{}
		err := json.Unmarshal(message.Data, &input)
		if err != nil {
			return nil, err
		}

		return input, nil
	}

	parsedEvent, err := IntermediateAsSpEnrichedParsed(interState, message)
	if err != nil {
		return nil, err
	}

	spInput, err := parsedEvent.ToMap()
	if err != nil {
		return nil, err
	}

	return spInput, nil
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
