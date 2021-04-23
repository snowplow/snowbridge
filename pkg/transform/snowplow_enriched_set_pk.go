// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// NewSpEnrichedSetPkFunction returns a TransformationFunction which sets the partition key of a message to a field within a Snowplow enriched event
func NewSpEnrichedSetPkFunction(pkField string) TransformationFunction {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, interface{}) {
		// To avoid parsing message multiple times, we check for intermediateState and save the parsed message to it if there is none.
		// Note that this will overwrite any differently typed intermediateState - in such a case order of execution matters.
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
		pk, err := parsedMessage.GetValue(pkField)
		if err != nil {
			message.SetError(err)
			return nil, message, nil
		}
		message.PartitionKey = fmt.Sprintf("%v", pk)
		return message, nil, intermediateState
	}
}
