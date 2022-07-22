// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/dlclark/regexp2"

	"github.com/pkg/errors"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
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

func evaluateSpEnrichedFilter(valuesFound []interface{}, regex string, regexTimeout int) bool {
	re, err := regexp2.Compile(regex, 0)
	re.MatchTimeout = time.Duration(regexTimeout) * time.Second
	if err != nil {
		log.Fatal(errors.Wrap(err, `error compiling regex for filter`))
	}
	for _, v := range valuesFound {
		if ok, _ := re.MatchString(fmt.Sprintf("%v", v)); ok {
			return true
		}
	}
	return false
}

// createSpEnrichedFilterFunction returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event
// and a regex declared by the user.
func createSpEnrichedFilterFunction(field, regex string, regexTimeout int, isUnstructEvent bool) (TransformationFunction, error) {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		if regexTimeout == 0 {
			// default timeout for regex is 10 seconds
			regexTimeout = 10
		}
		// Evaluate intermediateState to parsedEvent
		parsedMessage, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		// This regex retrieves the path fields
		// (e.g. field1.field2[0].field3 -> [field1, field2, 0, field3])
		regexWords := `\w+`
		re := regexp.MustCompile(regexWords)

		// separate the path string into words using regex
		path := re.FindAllString(field, -1)
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
			field,
			parsedMessage,
			convertPathToInterfaces(separatedPath),
		)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		// evaluate whether the found value passes the filter, determining if the message should be kept
		shouldKeepMessage := evaluateSpEnrichedFilter(valueFound, regex, regexTimeout)

		// if message is not to be kept, return it as a filtered message to be acked in the main function
		if !shouldKeepMessage {
			return nil, message, nil, nil
		}

		// otherwise, return the message and intermediateState for further processing.
		return message, nil, nil, parsedMessage
	}, nil
}

// NewSpEnrichedFilterFunction returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
func NewSpEnrichedFilterFunction(field, regex string, regexTimeout int) (TransformationFunction, error) {
	return createSpEnrichedFilterFunction(field, regex, regexTimeout, false)
}

// NewSpEnrichedFilterFunctionContext returns a TransformationFunction for filtering a context
func NewSpEnrichedFilterFunctionContext(field, regex string, regexTimeout int) (TransformationFunction, error) {
	return createSpEnrichedFilterFunction(field, regex, regexTimeout, false)
}

// NewSpEnrichedFilterFunctionUnstructEvent returns a TransformationFunction for filtering an unstruct_event
func NewSpEnrichedFilterFunctionUnstructEvent(field, regex string, regexTimeout int) (TransformationFunction, error) {
	return createSpEnrichedFilterFunction(field, regex, regexTimeout, true)
}
