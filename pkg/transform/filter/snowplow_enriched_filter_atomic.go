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

package filter

import (
	"github.com/pkg/errors"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// AtomicFilterConfig is a configuration object for the spEnrichedFilter transformation
type AtomicFilterConfig struct {
	AtomicField  string `hcl:"atomic_field"`
	Regex        string `hcl:"regex"`
	FilterAction string `hcl:"filter_action"`
}

// The adapter type is an adapter for functions to be used as
// pluggable components for spEnrichedFilter transformation. It implements the Pluggable interface.
type atomicFilterAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f atomicFilterAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f atomicFilterAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &AtomicFilterConfig{}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedFilter transformation adapter.
func atomicFilterAdapterGenerator(f func(c *AtomicFilterConfig) (transform.TransformationFunction, error)) atomicFilterAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*AtomicFilterConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// atomicFilterConfigFunction returns an spEnrichedFilter transformation function, from an atomicFilterConfig.
func atomicFilterConfigFunction(c *AtomicFilterConfig) (transform.TransformationFunction, error) {
	return NewAtomicFilterFunction(
		c.AtomicField,
		c.Regex,
		c.FilterAction,
	)
}

// AtomicFilterConfigPair is a configuration pair for the spEnrichedSetPk transformation
var AtomicFilterConfigPair = config.ConfigurationPair{
	Name:   "spEnrichedFilter",
	Handle: atomicFilterAdapterGenerator(atomicFilterConfigFunction),
}

// makeBaseValueGetter returns a valueGetter for base-level values.
// Because the different types of filter require different arguments, we use a constructor to produce a valueGetter.
// This allows them to be plugged into the createFilterFunction constructor.
func makeBaseValueGetter(field string) valueGetter {
	return func(parsedEvent analytics.ParsedEvent) (value []interface{}, err error) {
		// find the value in the event
		valueFound, err := parsedEvent.GetValue(field)
		// We don't return an error for empty field since this just means the value is nil.
		if err != nil && err.Error() != analytics.EmptyFieldErr {
			return nil, err
		}
		return []interface{}{valueFound}, nil
	}
}

// NewAtomicFilterFunction returns a transform.TransformationFunction which filters messages based on a field in the Snowplow enriched event.
func NewAtomicFilterFunction(field, regex string, filterAction string) (transform.TransformationFunction, error) {

	// Validate the field provided
	err := transform.ValidateAtomicField(field)
	if err != nil {
		return nil, err
	}
	// getBaseValueForMatch is responsible for retrieving data from the message for base fields
	getBaseValueForMatch := makeBaseValueGetter(field)

	return createFilterFunction(regex, getBaseValueForMatch, filterAction)
}
