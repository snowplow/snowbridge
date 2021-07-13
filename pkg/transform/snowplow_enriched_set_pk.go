// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// NewSpEnrichedSetPkFunction returns a TransformationFunction which sets the partition key of a message to a field within a Snowplow enriched event
func NewSpEnrichedSetPkFunction(pkField string) TransformationFunction {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// Evalute intermediateState to parsedEvent
		parsedMessage, parseErr := intermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		pk, err := parsedMessage.GetValue(pkField)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}
		message.PartitionKey = fmt.Sprintf("%v", pk)
		return message, nil, nil, parsedMessage
	}
}
