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

// NewSpEnrichedFilter returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
// The filterconfig should describe the conditions for including a message.
// For example "aid=abc|def" includes all events with app IDs of abc or def, and filters out the rest.
// aid!=abc|def includes all events whose app IDs do not match abc or def, and filters out the rest.
func NewSpEnrichedFilterFunction(filterConfig string) (TransformationFunction, error) {

	// This regex prevents whitespace characters in the value provided
	regex := `\S+(!=|==)[^\s\|]+((?:\|[^\s|]+)*)$`
	re := regexp.MustCompile(regex)

	if !(re.MatchString(filterConfig)) {
		// If invalid, return an error which will be returned by the main function
		return nil, errors.New(fmt.Sprintf("Filter Function Config does not match regex %v", regex))
	}

	// Check for a negation condition first
	keyValues := strings.SplitN(filterConfig, "!=", 2)

	// Initial Keep Value is the t/f value of keepMessage, to be reset on every invocation of the returned function
	var initialKeepValue bool
	if len(keyValues) > 1 {
		// If negation condition is found, default to keep the message, and change this when match found
		initialKeepValue = true
	} else {
		// Otherwise, look for affirmation condition, default to drop the message and change when match found
		keyValues = strings.SplitN(filterConfig, "==", 2)
		initialKeepValue = false
	}

	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// Start by resetting keepMessage to initialKeepValue
		keepMessage := initialKeepValue

		// Evalute intermediateState to parsedEvent
		parsedMessage, parseErr := intermediateAsParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		valueFound, err := parsedMessage.GetValue(keyValues[0])
		// TODO: What happens if the key doesn't exist? What should happen?
		// For a != condition, I think the behaviour should be different to that of an == condition (pass for !=, fail for == ...)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

	evaluation:
		for _, valueToMatch := range strings.Split(keyValues[1], "|") {
			if valueToMatch == fmt.Sprintf("%v", valueFound) { // coerce to string as valueFound may be any type found in a Snowplow event
				keepMessage = !keepMessage
				break evaluation
				// Once config value is matched once, change keepMessage then break out of the loop to avoid reverting back when we have two matches
			}
		}

		// If message is not to be kept, return it as a filtered message to be acked in the main function
		if !keepMessage {

			return nil, message, nil, nil
		}

		// Otherwise, return the message and intermediateState for further processing.
		return message, nil, nil, parsedMessage
	}, nil
}
