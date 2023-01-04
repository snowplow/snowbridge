//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package filter

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// ContextFilterConfig is a configuration object for the spEnrichedFilterContext transformation
type ContextFilterConfig struct {
	ContextFullName string `hcl:"context_full_name"`
	CustomFieldPath string `hcl:"custom_field_path"`
	Regex           string `hcl:"regex"`
	FilterAction    string `hcl:"filter_action"`
}

// The adapter type is an adapter for functions to be used as
// pluggable components for spEnrichedFilterContext transformation. It implements the Pluggable interface.
type contextFilterAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f contextFilterAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f contextFilterAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &ContextFilterConfig{}

	return cfg, nil
}

// contextFilterAdapterGenerator returns a Context Filter adapter.
func contextFilterAdapterGenerator(f func(c *ContextFilterConfig) (transform.TransformationFunction, error)) contextFilterAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*ContextFilterConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// contextFilterConfigFunction returns an spEnrichedFilterContext transformation function, from a contextFilterConfig.
func contextFilterConfigFunction(c *ContextFilterConfig) (transform.TransformationFunction, error) {
	return NewContextFilter(
		c.ContextFullName,
		c.CustomFieldPath,
		c.Regex,
		c.FilterAction,
	)
}

// ContextFilterConfigPair is a configuration pair for the spEnrichedSetPk transformation
var ContextFilterConfigPair = config.ConfigurationPair{
	Name:   "spEnrichedFilterContext",
	Handle: contextFilterAdapterGenerator(contextFilterConfigFunction),
}

// makeContextValueGetter creates a valueGetter for context data.
// Because the different types of filter require different arguments, we use a constructor to produce a valueGetter.
// This allows them to be plugged into the createFilterFunction constructor.
func makeContextValueGetter(name string, path []interface{}) valueGetter {
	return func(parsedEvent analytics.ParsedEvent) ([]interface{}, error) {
		value, err := parsedEvent.GetContextValue(name, path...)
		// We don't return an error for empty field since this just means the value is nil.
		if err != nil && err.Error() != analytics.EmptyFieldErr {
			return nil, err
		}
		// bug in analytics sdk requires the type casting below. https://github.com/snowplow/snowplow-golang-analytics-sdk/issues/36
		// GetContextValue should always return []interface{} but instead it returns an interface{} which always contains type []interface{}

		// if it's nil, return nil - we just didn't find any value.
		if value == nil {
			return nil, nil
		}
		// otherwise, type assertion.
		valueFound, ok := value.([]interface{})
		if !ok {
			return nil, errors.New(fmt.Sprintf("Context filter encountered unexpected type in getting value for path %v", path))
		}

		return valueFound, nil
	}
}

// NewContextFilter returns a transform.TransformationFunction for filtering data based on values in a context
func NewContextFilter(contextFullName, pathToField, regex string, filterAction string) (transform.TransformationFunction, error) {
	path, err := parsePathToArguments(pathToField)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Context filter function")
	}

	// getContextValuesForMatch is responsible for retrieving data from the message for context fields
	getContextValuesForMatch := makeContextValueGetter(contextFullName, path)

	return createFilterFunction(regex, getContextValuesForMatch, filterAction)
}
