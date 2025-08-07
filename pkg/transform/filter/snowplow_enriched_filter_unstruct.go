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
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// UnstructFilterConfig is a configuration object for the spEnrichedFilterUnstructEvent transformation
type UnstructFilterConfig struct {
	CustomFieldPath           string `hcl:"custom_field_path"`
	UnstructEventName         string `hcl:"unstruct_event_name"`
	UnstructEventVersionRegex string `hcl:"unstruct_event_version_regex,optional"`
	Regex                     string `hcl:"regex"`
	FilterAction              string `hcl:"filter_action"`
}

// The adapter type is an adapter for functions to be used as
// pluggable components for spEnrichedFilterUnstructEvent transformation. It implements the Pluggable interface.
type unstructFilterAdapter func(i any) (any, error)

// Create implements the ComponentCreator interface.
func (f unstructFilterAdapter) Create(i any) (any, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface
func (f unstructFilterAdapter) ProvideDefault() (any, error) {
	// Provide defaults
	cfg := &UnstructFilterConfig{
		UnstructEventVersionRegex: ".*",
	}

	return cfg, nil
}

// adapterGenerator returns a spEnrichedFilterUnstructEvent transformation adapter.
func unstructFilterAdapterGenerator(f func(c *UnstructFilterConfig) (transform.TransformationFunction, error)) unstructFilterAdapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*UnstructFilterConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// unstructFilterConfigFunction returns an spEnrichedFilterUnstructEvent transformation function, from an unstructFilterConfig.
func unstructFilterConfigFunction(c *UnstructFilterConfig) (transform.TransformationFunction, error) {
	return NewUnstructFilter(
		c.UnstructEventName,
		c.UnstructEventVersionRegex,
		c.CustomFieldPath,
		c.Regex,
		c.FilterAction,
	)
}

// UnstructFilterConfigPair is a configuration pair for the spEnrichedSetPk transformation
var UnstructFilterConfigPair = config.ConfigurationPair{
	Name:   "spEnrichedFilterUnstructEvent",
	Handle: unstructFilterAdapterGenerator(unstructFilterConfigFunction),
}

// makeUnstructValueGetter creates a valueGetter for unstruct data.
// Because the different types of filter require different arguments, we use a constructor to produce a valueGetter.
// This allows them to be plugged into the createFilterFunction constructor.
func makeUnstructValueGetter(eventName string, versionRegex *regexp.Regexp, path []any) valueGetter {
	return func(parsedEvent analytics.ParsedEvent) (value []any, err error) {
		eventNameFound, err := parsedEvent.GetValue(`event_name`)
		if err != nil { // This field can't be empty for a valid event, so we return all errors here
			return nil, err
		}
		if eventNameFound != eventName { // If we don't have an exact match on event name, we return nil value
			return nil, nil
		}
		versionFound, err := parsedEvent.GetValue(`event_version`)
		if err != nil { // This field can't be empty for a valid event, so we return all errors here
			return nil, err
		}
		if !versionRegex.MatchString(versionFound.(string)) { // If we don't match the provided version regex, return nil value
			return nil, nil
		}

		valueFound, err := parsedEvent.GetUnstructEventValue(path...)
		// We don't return an error for empty field since this just means the value is nil.
		if err != nil && err.Error() != analytics.EmptyFieldErr && !strings.Contains(err.Error(), "not found") {
			// This last clause exists because of this: https://github.com/snowplow/snowplow-golang-analytics-sdk/issues/37
			// TODO: Fix that and remove it as soon as possible.
			return nil, err
		}

		if valueFound == nil {
			return nil, nil
		}

		return []any{valueFound}, nil
	}
}

// NewUnstructFilter returns a transform.TransformationFunction for filtering an unstruct_event
func NewUnstructFilter(eventNameToMatch, eventVersionToMatch, pathToField, regex string, filterAction string) (transform.TransformationFunction, error) {
	path, err := parsePathToArguments(pathToField)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Unstruct filter function")
	}

	versionRegex, err := regexp.Compile(eventVersionToMatch)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprint("Failed to compile regex: ", eventVersionToMatch))
	}

	// getUnstructValuesForMatch is responsible for retrieving data from the message for unstruct fields.
	// It also checks that the correct event name and version are provided, and returns nil if not.
	getUnstructValuesForMatch := makeUnstructValueGetter(eventNameToMatch, versionRegex, path)

	return createFilterFunction(regex, getUnstructValuesForMatch, filterAction)
}
