// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transformiface

import "github.com/snowplow-devops/stream-replicator/pkg/models"

type TransformationFunction func([]*models.Message) ([]*models.Message, []*models.Message)

type TransformationApplyFunction func([]*models.Message) *models.TransformationResult

type TransformationGenerator func(...TransformationFunction) TransformationApplyFunction

// NewTransformation constructs a function which applies all transformations to all messages, returning a TransformationResult.
func NewTransformation(tranformFunctions ...TransformationFunction) TransformationApplyFunction {
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
