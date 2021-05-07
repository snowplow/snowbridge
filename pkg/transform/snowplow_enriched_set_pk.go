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
	return func(message *models.Message) (*models.Message, *models.Message) {
		parsedMessage, err := analytics.ParseEvent(string(message.Data))
		if err != nil {
			message.SetError(err)
			return nil, message
		}
		pk, err := parsedMessage.GetValue(pkField)
		if err != nil {
			message.SetError(err)
			return nil, message
		}
		newMessage := *message
		newMessage.PartitionKey = fmt.Sprintf("%v", pk) // Cheeky way to wrangle interface into string. Is it problematic?
		return &newMessage, nil
	}
}
