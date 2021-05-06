// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// SpEnrichedToJson is a specific transformation implementation to transform good enriched data within a message to Json
func SpEnrichedToJson(message *models.Message) (*models.Message, *models.Message) {
	parsedMessage, err := analytics.ParseEvent(string(message.Data))
	if err != nil {
		message.SetError(err)
		return nil, message
	}
	jsonMessage, err := parsedMessage.ToJson()
	if err != nil {
		message.SetError(err)
		return nil, message
	}
	newMessage := *message
	newMessage.Data = jsonMessage // TODO: test if it's significantly faster to return pointer and edit-in-place
	newMessage.TimeTransformed = time.Now().UTC()
	return &newMessage, nil
}
