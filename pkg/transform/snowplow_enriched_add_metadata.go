// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// NewSpEnrichedAddMetadataFunction returns a TransformationFunction which adds metadata to a message from a field within a Snowplow enriched event
func NewSpEnrichedAddMetadataFunction(key, field string) TransformationFunction {
	return func(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// Evaluate intermediateState to parsedEvent
		parsedMessage, parseErr := IntermediateAsSpEnrichedParsed(intermediateState, message)
		if parseErr != nil {
			message.SetError(parseErr)
			return nil, nil, message, nil
		}

		value, err := parsedMessage.GetValue(field)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}
		if message.Metadata == nil {
			message.Metadata = map[string]interface{}{
				key: fmt.Sprintf("%v", value),
			}
		} else {
			message.Metadata[key] = fmt.Sprintf("%v", value)
		}
		return message, nil, nil, parsedMessage
	}
}
