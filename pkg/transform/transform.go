// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// TransformationFunctions modify their inputs
type TransformationFunction func(*models.Message) (*models.Message, *models.Message)

// The transformationApplyFunction dereferences messages before running transformations
type TransformationApplyFunction func([]*models.Message) *models.TransformationResult

type TransformationGenerator func(...TransformationFunction) TransformationApplyFunction

// NewTransformation constructs a function which applies all transformations to all messages, returning a TransformationResult.
func NewTransformation(tranformFunctions ...TransformationFunction) TransformationApplyFunction {
	return func(messages []*models.Message) *models.TransformationResult {
		successes := make([]*models.Message, 0, len(messages))
		failures := make([]*models.Message, 0, len(messages))
		// If no transformations, just return the result rather than shuffling data between slices
		if len(tranformFunctions) == 0 {
			return models.NewTransformationResult(messages, failures)
		}

		for _, message := range messages {
			msg := *message // dereference to avoid amending input
			success := &msg // success must be both input and output to a TransformationFunction, so we make this pointer.
			var failure *models.Message
			for _, transformFunction := range tranformFunctions {
				// Overwrite the input for each iteration in sequence of transformations,
				// since the desired result is a single transformed message with a nil failure, or a nil message with a single failure
				success, failure = transformFunction(success)
				if failure != nil {
					break
				}
			}
			if success != nil {
				success.TimeTransformed = time.Now().UTC()
				successes = append(successes, success)
			}
			if failure != nil {
				// We don't append TimeTransformed in the failure case, as it is less useful, and likely to skew metrics
				failures = append(failures, failure)
			}
		}
		return models.NewTransformationResult(successes, failures)
	}
}
