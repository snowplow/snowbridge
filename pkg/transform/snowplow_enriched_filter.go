// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

func findSpEnrichedFilterValue(queriedField, parsedEventName, eventVer, field string, parsedMessage analytics.ParsedEvent, path []interface{}) ([]interface{}, error) {
	var vf interface{}
	var valueFound []interface{}
	var err error

	switch {
	case strings.HasPrefix(queriedField, `contexts_`):
		vf, err = parsedMessage.GetContextValue(queriedField, path...)
		valueFound = append(valueFound, vf.([]interface{})...)
	case strings.HasPrefix(queriedField, `unstruct_event`):
		eventNameFull := `unstruct_event_` + parsedEventName
		if queriedField == eventNameFull || queriedField == eventNameFull+`_`+eventVer {
			vf, err = parsedMessage.GetUnstructEventValue(path...)
			valueFound = append(valueFound, vf)
		}
	default:
		vf, err = parsedMessage.GetValue(field)
		valueFound = append(valueFound, vf)
	}
	if err != nil {
		// GetValue returns an error if the field requested is empty. Check for that particular error before returning error
		if err.Error() == analytics.EmptyFieldErr {
			return nil, nil
		}
		return nil, err
	}
	return valueFound, nil
}

func evaluateSpEnrichedFilter(valuesToMatch string, valuesFound []interface{}, isNegationFilter, shouldKeepMessage *bool) {
	for _, valueToMatch := range strings.Split(valuesToMatch, "|") {
		for _, v := range valuesFound {
			if fmt.Sprintf("%v", v) == valueToMatch {
				// Once config value is matched once, change shouldKeepMessage, and stop looking for matches
				if *isNegationFilter {
					*shouldKeepMessage = false
				} else {
					*shouldKeepMessage = true
				}
				return

			}
		}
	}
}

// createSpEnrichedFilterFunction returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
// The filterconfig should describe the conditions for including a message.
// For example "aid=abc|def" includes all events with app IDs of abc or def, and filters out the rest.
// aid!=abc|def includes all events whose app IDs do not match abc or def, and filters out the rest.
func createSpEnrichedFilterFunction(filterConfig string, isUnstructEvent bool, isContext bool) (TransformationFunction, error) {
	// This regex prevents whitespace characters in the value provided
	regex := `\S+(!=|==)[^\s\|]+((?:\|[^\s|]+)*)$`
	re := regexp.MustCompile(regex)

	if !(re.MatchString(filterConfig)) {
		// If invalid, return an error which will be returned by the main function
		return nil, errors.New("invalid filter function config, must be of the format {field name}=={value}[|{value}|...] or {field name}!={value}[|{value}|...]")
	}

	// Check for a negation condition first
	keyValues := strings.SplitN(filterConfig, "!=", 2)

	// isNegationFilter determines whether a match sets shouldKeepMessage to true or false, and consequently whether message is kept or filtered
	var isNegationFilter bool
	if len(keyValues) > 1 {
		// If negation condition is found, default to keep the message, and change this when match found
		isNegationFilter = true
	} else {
		// Otherwise, look for affirmation condition, default to drop the message and change when match found
		keyValues = strings.SplitN(filterConfig, "==", 2)
		isNegationFilter = false
	}

	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// Start by resetting shouldKeepMessage to isNegationFilter
		shouldKeepMessage := isNegationFilter

		// Evaluate intermediateState to parsedEvent
		parsedMessage, parseErr := intermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		// This regex retrieves the path fields
		// (e.g. field1.field2[0].field3 -> [field1, field2, 0, field3])
		regex = `\w+`
		re = regexp.MustCompile(regex)

		// separate the path string into words using regex
		path := re.FindAllString(keyValues[0], -1)
		separatedPath := make([]string, len(path)-1)
		for idx, pathField := range path[1:] {
			separatedPath[idx] = pathField
		}

		var parsedEventName string
		var eventMajorVer string
		var err error

		// only call SDK functions if an unstruct_event is being filtered
		if isUnstructEvent {
			// get event name
			eventName, err := parsedMessage.GetValue(`event_name`)
			if err != nil {
				message.SetError(err)
				return nil, nil, message, nil
			}
			parsedEventName = eventName.(string)
			// get event version
			fullEventVer, err := parsedMessage.GetValue(`event_version`)
			if err != nil {
				message.SetError(err)
				return nil, nil, message, nil
			}
			// get the major event version
			eventMajorVer = strings.Split(fullEventVer.(string), `-`)[0]
			if eventMajorVer == `` {
				message.SetError(fmt.Errorf(`invalid schema version format: %s`, fullEventVer))
				return nil, nil, message, nil
			}
		}

		// find the value in the event
		valueFound, err := findSpEnrichedFilterValue(
			path[0],
			parsedEventName,
			eventMajorVer,
			keyValues[0],
			parsedMessage,
			convertPathToInterfaces(separatedPath),
		)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		// evaluate whether the found value passes the filter, determining if the message should be kept
		evaluateSpEnrichedFilter(keyValues[1], valueFound, &isNegationFilter, &shouldKeepMessage)

		// if message is not to be kept, return it as a filtered message to be acked in the main function
		if !shouldKeepMessage {
			return nil, message, nil, nil
		}

		// otherwise, return the message and intermediateState for further processing.
		return message, nil, nil, parsedMessage
	}, nil
}

// NewSpEnrichedFilterFunction returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
func NewSpEnrichedFilterFunction(filterConfig string) (TransformationFunction, error) {
	return createSpEnrichedFilterFunction(filterConfig, false, false)
}

// NewSpEnrichedFilterFunctionContext returns a TransformationFunction for filtering a context
func NewSpEnrichedFilterFunctionContext(filterConfig string) (TransformationFunction, error) {
	return createSpEnrichedFilterFunction(filterConfig, false, true)
}

// NewSpEnrichedFilterFunctionUnstructEvent returns a TransformationFunction for filtering an unstruct_event
func NewSpEnrichedFilterFunctionUnstructEvent(filterConfig string) (TransformationFunction, error) {
	return createSpEnrichedFilterFunction(filterConfig, true, false)
}
