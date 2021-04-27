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

// Should NewTransformation live somewhere else and if so where? It's written to be general to applying _any_ set of transformations.

// NewTransformation constructs a function which applies all transformations to all messages, returning a TransformationResult.
func NewTransformation(tranformFunctions ...transformiface.TransformationFunction) func(messages []*models.Message) *models.TransformationResult {
	return func(messages []*models.Message) *models.TransformationResult {
		successes := messages
		failures := make([]*models.Message, 0, len(messages))

		for _, transformFunction := range tranformFunctions {
			success, failure := transformFunction(messages)
			// no error as errors should be returned in the 'Invalid' slice of TransformationResult
			failures = append(failures, failure...)
			successes = success
		}
		return models.NewTransformationResult(successes, failures)
	}
}

// EnrichedToJson is a specific transformation implementation to transform good enriched data within a message to Json
func EnrichedToJson(messages []*models.Message) ([]*models.Message, []*models.Message) {
	successes := make([]*models.Message, 0, len(messages))
	failures := make([]*models.Message, 0, len(messages))

	for _, message := range messages {
		parsedMessage, err := analytics.ParseEvent(string(message.Data))
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
			continue
		}
		JsonMessage, err := parsedMessage.ToJson()
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
			continue
		}
		message.Data = JsonMessage // because we're using a pointer, this alters the original value I think. Is this is acceptable?
		successes = append(successes, message)

	}
	return successes, failures // Doesn't return any err as errors should all go into failures.
}
