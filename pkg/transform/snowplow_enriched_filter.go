// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dlclark/regexp2"

	"github.com/pkg/errors"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func evaluateSpEnrichedFilter(re *regexp2.Regexp, valuesFound []interface{}) bool {
	// if valuesFound is nil, we found no value.
	// Because negative matches are a thing, we still want to match against an empty string
	if valuesFound == nil {
		valuesFound = make([]interface{}, 1)
	}
	for _, v := range valuesFound {
		if v == nil {
			v = "" // because nil gets cast to `<nil>`
		}

		if ok, _ := re.MatchString(fmt.Sprintf("%v", v)); ok {
			return true
		}
	}
	return false
}

func createSpEnrichedFilterFunction(regex string, regexTimeout int, getFunc valueGetter) (TransformationFunction, error) {
	if regexTimeout == 0 {
		// default timeout for regex is 10 seconds
		regexTimeout = 10
	}

	// regexToMatch is what we use to evaluate the actual filter, once we have the value.
	regexToMatch, err := regexp2.Compile(regex, 0)
	regexToMatch.MatchTimeout = time.Duration(regexTimeout) * time.Second
	if err != nil {
		return nil, errors.Wrap(err, `error compiling regex for filter`)
	}

	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {

		// Evaluate intermediateState to parsedEvent
		parsedMessage, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		// get the value
		valueFound, err := getFunc(parsedMessage)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		// evaluate whether the found value passes the filter, determining if the message should be kept
		shouldKeepMessage := evaluateSpEnrichedFilter(regexToMatch, valueFound)

		// if message is not to be kept, return it as a filtered message to be acked in the main function
		if !shouldKeepMessage {
			return nil, message, nil, nil
		}

		// otherwise, return the message and intermediateState for further processing.
		return message, nil, nil, parsedMessage
	}, nil
}

// valueGetter is a function that can hold the logic for getting values in the case of base, context, and unstruct fields,
// which respecively require different logic.
type valueGetter func(analytics.ParsedEvent) ([]interface{}, error)

// Because each type of value requires different arguments, we use these `make` functions to construct them.
// This allows us to unit test each one, plug them into the createSpEnrichedFilterFunction constructor,
// and to construct them so that field names/paths and regexes are handled only once, at startup.

// makeBaseValueGetter returns a valueGetter for base-level values.
func makeBaseValueGetter(field string) valueGetter {
	return func(parsedMessage analytics.ParsedEvent) (value []interface{}, err error) {
		// find the value in the event
		valueFound, err := parsedMessage.GetValue(field)
		// We don't return an error for empty field since this just means the value is nil.
		if err != nil && err.Error() != analytics.EmptyFieldErr {
			return nil, err
		}
		return []interface{}{valueFound}, nil
	}
}

// NewSpEnrichedFilterFunction returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
func NewSpEnrichedFilterFunction(field, regex string, regexTimeout int) (TransformationFunction, error) {

	// getBaseValueForMatch is responsible for retrieving data from the message for base fields
	getBaseValueForMatch := makeBaseValueGetter(field)

	return createSpEnrichedFilterFunction(regex, regexTimeout, getBaseValueForMatch)
}

// makeContextValueGetter creates a valueGetter for context data
func makeContextValueGetter(name string, path []interface{}) valueGetter {
	return func(parsedMessage analytics.ParsedEvent) ([]interface{}, error) {
		value, err := parsedMessage.GetContextValue(name, path...)
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

// NewSpEnrichedFilterFunctionContext returns a TransformationFunction for filtering a context
func NewSpEnrichedFilterFunctionContext(contextFullName, pathToField, regex string, regexTimeout int) (TransformationFunction, error) {

	path, err := parsePathToArguments(pathToField)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Context filter function")
	}

	// getContextValuesForMatch is responsible for retrieving data from the message for context fields
	getContextValuesForMatch := makeContextValueGetter(contextFullName, path)

	return createSpEnrichedFilterFunction(regex, regexTimeout, getContextValuesForMatch)
}

// makeUnstructValueGetter creates a valueGetter for unstruct data.
func makeUnstructValueGetter(eventName string, versionRegex *regexp.Regexp, path []interface{}) valueGetter {
	return func(parsedMessage analytics.ParsedEvent) (value []interface{}, err error) {
		eventNameFound, err := parsedMessage.GetValue(`event_name`)
		if err != nil { // This field can't be empty for a valid event, so we return all errors here
			return nil, err
		}
		if eventNameFound != eventName { // If we don't have an exact match on event name, we return nil value
			return nil, nil
		}
		versionFound, err := parsedMessage.GetValue(`event_version`)
		if err != nil { // This field can't be empty for a valid event, so we return all errors here
			return nil, err
		}
		if !versionRegex.MatchString(versionFound.(string)) { // If we don't match the provided version regex, return nil value
			return nil, nil
		}

		valueFound, err := parsedMessage.GetUnstructEventValue(path...)
		// We don't return an error for empty field since this just means the value is nil.
		if err != nil && err.Error() != analytics.EmptyFieldErr && !strings.Contains(err.Error(), "not found") {
			// This last clause exists because of this: https://github.com/snowplow/snowplow-golang-analytics-sdk/issues/37
			// TODO: Fix that and remove it as soon as possible.
			return nil, err
		}

		if valueFound == nil {
			return nil, nil
		}

		return []interface{}{valueFound}, nil
	}
}

// NewSpEnrichedFilterFunctionUnstructEvent returns a TransformationFunction for filtering an unstruct_event
func NewSpEnrichedFilterFunctionUnstructEvent(eventNameToMatch, eventVersionToMatch, pathToField, regex string, regexTimeout int) (TransformationFunction, error) {

	path, err := parsePathToArguments(pathToField)
	if err != nil {
		return nil, errors.Wrap(err, "error creating Unstruct filter function")
	}

	versionRegex, err := regexp.Compile(eventVersionToMatch)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprint("Failed to compile regex: ", eventVersionToMatch))
	}

	// getUnstructValuesForMatch is responsible for retrieving data from the message for context fields.
	// It also checks that the correct event name and version are provided, and returns nil if not.
	getUnstructValuesForMatch := makeUnstructValueGetter(eventNameToMatch, versionRegex, path)

	return createSpEnrichedFilterFunction(regex, regexTimeout, getUnstructValuesForMatch)
}

// parsePathToArguments parses a string path to custom data (eg. `test1.test2[0].test3`)
// into the slice of interfaces expected by the analytics SDK's Get() methods.
func parsePathToArguments(pathToField string) ([]interface{}, error) {
	// validate that an edge case (unmatched opening brace) isn't present
	if strings.Count(pathToField, "[") != strings.Count(pathToField, "]") {
		return nil, errors.New(fmt.Sprint("unmatched brace in path: ", pathToField))
	}

	// regex to separate path into components
	re := regexp.MustCompile(`\[\d+\]|[^\.\[]+`)
	parts := re.FindAllString(pathToField, -1)

	// regex to identify arrays
	arrayRegex := regexp.MustCompile(`\[\d+\]`)

	convertedPath := make([]interface{}, 0)
	for _, part := range parts {

		if arrayRegex.MatchString(part) { // handle arrays first
			intPart, err := strconv.Atoi(part[1 : len(part)-1]) // strip braces and convert to int
			if err != nil {
				return nil, errors.New(fmt.Sprint("error parsing path element: ", part))
			}

			convertedPath = append(convertedPath, intPart)
		} else { // handle strings
			convertedPath = append(convertedPath, part)
		}

	}
	return convertedPath, nil
}
