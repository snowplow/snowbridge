// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformiface"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// NewTransformation constructs a function which applies a sequence of transformations to messages, and returns a TransformationResult.
func NewTransformation(tranformFunctions ...transformiface.TransformationFunction) func(messages []*models.Message) (*models.TransformationResult, error) {
	return func(messages []*models.Message) (*models.TransformationResult, error) {
		successes := messages
		failures := make([]*models.Message, 0, len(messages))

		for _, transformFunction := range tranformFunctions {
			success, failure, err := transformFunction(messages)
			if err != nil { // TODO: Figure out error handling...
				// do something
			}
			failures = append(failures, failure...)
			successes = success
		}
		return models.NewTransformationResult(successes, failures), nil
	}
} // This seems generic enough that perhaps it should live elsewhere? If we were to create a set of transformations on raw data or some other format, for example, this exact same function would be used.

// EnrichedToJson is a specific transformation implementation to transform good enriched data within a message to Json
func EnrichedToJson(messages []*models.Message) ([]*models.Message, []*models.Message, error) {
	successes := make([]*models.Message, 0, len(messages))
	failures := make([]*models.Message, 0, len(messages))

	for _, message := range messages {
		parsedMessage, err := analytics.ParseEvent(string(message.Data))
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
		}
		JsonMessage, err := parsedMessage.ToJson()
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
		} else {
			message.Data = JsonMessage
			successes = append(successes, message)
		}
	}
	return successes, failures, nil // TO DO: Figure out error handling...
}
