// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// NewSpEnrichedFilterFunction returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
// The filterconfig should describe the conditions for including a message.
// For example "aid=abc|def" includes all events with app IDs of abc or def, and filters out the rest.
// aid!=abc|def includes all events whose app IDs do not match abc or def, and filters out the rest.
func NewSpEnrichedFilterFunction(filterConfig string) (TransformationFunction, error) {

	// This regex prevents whitespace characters in the value provided
	regex := `\S+(!=|==)[^\s\|]+((?:\|[^\s|]+)*)$`
	re := regexp.MustCompile(regex)

	if !(re.MatchString(filterConfig)) {
		// If invalid, return an error which will be returned by the main function
		return nil, errors.New("Invalid filter function config, must be of the format {field name}=={value}[|{value}|...] or {field name}!={value}[|{value}|...]")
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

		// Evalute intermediateState to parsedEvent
		parsedMessage, parseErr := intermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		valueFound, err := parsedMessage.GetValue(keyValues[0])

		// GetValue returns an error if the field requested is empty. Check for that particular error before failing the message.
		if err != nil && err.Error() == fmt.Sprintf("Field %s is empty", keyValues[0]) {
			valueFound = nil
		} else if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

	evaluation:
		for _, valueToMatch := range strings.Split(keyValues[1], "|") {
			if valueToMatch == fmt.Sprintf("%v", valueFound) { // coerce to string as valueFound may be any type found in a Snowplow event
				if isNegationFilter {
					shouldKeepMessage = false
				} else {
					shouldKeepMessage = true
				}
				break evaluation
				// Once config value is matched once, change shouldKeepMessage, and stop looking for matches
			}
		}

		// If message is not to be kept, return it as a filtered message to be acked in the main function
		if !shouldKeepMessage {

			return nil, message, nil, nil
		}

		// Otherwise, return the message and intermediateState for further processing.
		return message, nil, nil, parsedMessage
	}, nil
}
