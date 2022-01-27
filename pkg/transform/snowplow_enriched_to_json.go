// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// SpEnrichedToJSON is a specific transformation implementation to transform good enriched data within a message to Json
func SpEnrichedToJSON(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
	// Evalute intermediateState to parsedEvent
	parsedMessage, parseErr := intermediateAsSpEnrichedParsed(intermediateState, message)
	if parseErr != nil {
		message.SetError(parseErr)
		return nil, nil, message, nil
	}

	jsonMessage, err := parsedMessage.ToJson()
	if err != nil {
		message.SetError(err)
		return nil, nil, message, nil
	}
	message.Data = jsonMessage
	return message, nil, nil, parsedMessage
}
