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
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
)

// evaluateSpEnrichedfilter takes a regex and a slice of values, and returns whether or not a value has been matched
// If a value is nil, it matches against the empty string (regardless of type)
// If the type is not string the value is cast to string using fmt.Sprintf()
func evaluateSpEnrichedFilter(re *regexp.Regexp, valuesFound []interface{}) bool {
	// if valuesFound is nil, we found no value.
	// Because negative matches are a thing, we still want to match against an empty string
	if valuesFound == nil {
		valuesFound = make([]interface{}, 1)
	}
	for _, v := range valuesFound {
		if v == nil {
			v = "" // because nil gets cast to `<nil>`
		}

		if ok := re.MatchString(fmt.Sprintf("%v", v)); ok {
			return true
		}
	}
	return false
}

// createFilterFunction is a generator which creates a Snowplow filter function.
// The difference between the three types of filter function are all to do with how data is retrieved. This generator allows
// us to provide a valueGetter to grab the value to match against, but keep the same logic for execution of the filter itself.
func createFilterFunction(regex string, getFunc valueGetter, filterAction string) (transform.TransformationFunction, error) {
	var dropIfMatched bool
	switch filterAction {
	case "drop":
		dropIfMatched = true
	case "keep":
		dropIfMatched = false
	default:
		return nil, fmt.Errorf("invalid filter action found: %s - must be 'keep' or 'drop'", filterAction)
	}

	// regexToMatch is what we use to evaluate the actual filter, once we have the value.
	regexToMatch, err := regexp.Compile(regex)
	if err != nil {
		return nil, errors.Wrap(err, `error compiling regex for filter`)
	}

	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {

		// Evaluate intermediateState to parsedEvent
		parsedEvent, parseErr := transform.IntermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "intermediate state cannot be parsed as parsedEvent",
				Err:         parseErr,
			})
			return nil, nil, message, nil
		}

		// get the value
		valueFound, err := getFunc(parsedEvent)
		if err != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "failed to get value of parsed event",
				Err:         err,
			})
			return nil, nil, message, nil
		}

		// evaluate whether the found value passes the filter, determining if the message should be kept
		matchesRegex := evaluateSpEnrichedFilter(regexToMatch, valueFound)

		// if message is not to be kept, return it as a filtered message to be acked in the main function
		if (!matchesRegex && !dropIfMatched) || (matchesRegex && dropIfMatched) {
			return nil, message, nil, nil
		}

		// otherwise, return the message and intermediateState for further processing.
		return message, nil, nil, parsedEvent
	}, nil
}

// valueGetter is a function that can hold the logic for getting values in the case of base, context, and unstruct fields,
// which respecively require different logic.
type valueGetter func(analytics.ParsedEvent) ([]interface{}, error)

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
