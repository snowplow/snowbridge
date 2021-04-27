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
func NewTransformation(tranformFunctions ...transformiface.TransformationFunction) func(messages []*models.Message) *models.TransformationResult {
	return func(messages []*models.Message) *models.TransformationResult {
		successes := messages
		failures := make([]*models.Message, 0, len(messages))

		for _, transformFunction := range tranformFunctions {
			success, failure := transformFunction(messages)
			// no error as errors should be returned in the failures array of TransformationResult
			failures = append(failures, failure...)
			successes = success
		}
		return models.NewTransformationResult(successes, failures)
	}
} // This seems generic enough that perhaps it should live elsewhere? If we were to create a set of transformations on raw data or some other format, for example, this exact same function would be used.

// EnrichedToJson is a specific transformation implementation to transform good enriched data within a message to Json
func EnrichedToJson(messages []*models.Message) ([]*models.Message, []*models.Message) { // Probably no need for error here actually. Any errored transformation should be returned in the failures slice.
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
			failures = append(failures, message) // use continue here as well and remove `else` below? Any reason for/against?
		} else {
			message.Data = JsonMessage // because we're using a pointer, this alters the original value I think. TODO: Check that this is acceptable
			successes = append(successes, message)
		}
	}
	return successes, failures // Doesn't return any err as errors should all go into failures.
}
