// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"
	"strings"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

//

// NewSpEnrichedFilter returns a TransformationFunction which filters messages based on a field in the Snowplow enriched event.
// The filterconfig should describe the conditions for including a message.
// For example "aid=abc|def" includes all events with app IDs of abc or def, and filters out the rest.
// aid!=abc|def includes all events whose app IDs do not match abc or def, and filters out the rest.
func NewSpEnrichedFilterFunction(filterConfig string) TransformationFunction {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, interface{}) {

		// Check for a negation condition first
		keyValues := strings.SplitN(filterConfig, "!=", 2)

		var keepMessage bool
		if len(keyValues) > 1 {
			// If negation condition is found, default to keep the message, and change this when match found
			keepMessage = true
		} else {
			// Otherwise, look for affirmation condition, default to drop the message and change when match found
			keyValues = strings.SplitN(filterConfig, "==", 2)
			keepMessage = false
		}
		// TODO: Design - Should there be validation of the input here, or perhaps in the config? Or at all?

		// Todo: make this its own function and DRY across all the transformations?
		var parsedMessage, ok = intermediateState.(analytics.ParsedEvent)
		var parseErr error
		if ok {
			parsedMessage = intermediateState.(analytics.ParsedEvent)
		} else {
			parsedMessage, parseErr = analytics.ParseEvent(string(message.Data))
			if parseErr != nil {
				message.SetError(parseErr)
				return nil, message, nil
			}
			intermediateState = parsedMessage
		}

		valueFound, err := parsedMessage.GetValue(keyValues[0])
		if err != nil {
			message.SetError(err)
			return nil, message, nil
		}

	evaluation:
		for _, valueToMatch := range strings.Split(keyValues[1], "|") {
			if valueToMatch == fmt.Sprintf("%v", valueFound) { // coerce to string as valueFound may be any type found in a Snowplow event
				keepMessage = !keepMessage
				break evaluation
				// Once config value is matched once, change keepMessage then break out of the loop to avoid reverting back when we have two matches
			}
		}

		// If message is not to be kept, ack it and return a nil result.
		if !keepMessage {
			if message.AckFunc != nil {
				message.AckFunc()
			}
			return nil, nil, nil
		}

		// Otherwise, return the message and intermediateState for further processing.
		return message, nil, intermediateState
	}
}
