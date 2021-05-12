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

type TransformationFunction func(*models.Message) (*models.Message, *models.Message)

type TransformationApplyFunction func([]*models.Message) *models.TransformationResult

type TransformationGenerator func(...TransformationFunction) TransformationApplyFunction

// NewTransformation constructs a function which applies all transformations to all messages, returning a TransformationResult.
func NewTransformation(tranformFunctions ...TransformationFunction) TransformationApplyFunction {
	return func(messages []*models.Message) *models.TransformationResult {
		successes := make([]*models.Message, 0, len(messages))
		failures := make([]*models.Message, 0, len(messages))
		// if no transformations, just return the result rather than shuffling data between slices
		if len(tranformFunctions) == 0 {
			return models.NewTransformationResult(messages, failures)
		}

		for _, message := range messages {
			// Overwrite the input for each message in sequence, unless we hit a failure
			success := message
			var failure *models.Message
			for _, transformFunction := range tranformFunctions {
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
				failures = append(failures, failure)
			}
		}
		return models.NewTransformationResult(successes, failures)
	}
}
