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
type TransformationFunction func(*models.Message, interface{}) (*models.Message, *models.Message, *models.Message, interface{})

// The transformationApplyFunction dereferences messages before running transformations
type TransformationApplyFunction func([]*models.Message) *models.TransformationResult

type TransformationGenerator func(...TransformationFunction) TransformationApplyFunction

// NewTransformation constructs a function which applies all transformations to all messages, returning a TransformationResult.
func NewTransformation(tranformFunctions ...TransformationFunction) TransformationApplyFunction {
	return func(messages []*models.Message) *models.TransformationResult {
		successList := make([]*models.Message, 0, len(messages))
		filteredList := make([]*models.Message, 0, len(messages))
		failureList := make([]*models.Message, 0, len(messages))
		// If no transformations, just return the result rather than shuffling data between slices
		if len(tranformFunctions) == 0 {
			return models.NewTransformationResult(messages, filteredList, failureList)
		}

		for _, message := range messages {
			msg := *message // dereference to avoid amending input
			success := &msg // success must be both input and output to a TransformationFunction, so we make this pointer.
			var failure *models.Message
			var filtered *models.Message
			var intermediate interface{}
			for _, transformFunction := range tranformFunctions {
				// Overwrite the input for each iteration in sequence of transformations,
				// since the desired result is a single transformed message with a nil failure, or a nil message with a single failure
				success, filtered, failure, intermediate = transformFunction(success, intermediate)
				if failure != nil || filtered != nil {
					break
				}
			}
			if success != nil {
				success.TimeTransformed = time.Now().UTC()
				successList = append(successList, success)
			}
			if filtered != nil {
				filtered.TimeTransformed = time.Now().UTC() // TODO: Decide if we should amend the model for observability here and implement TimeFiltered instead?
				filteredList = append(filteredList, filtered)
			}
			if failure != nil {
				// We don't append TimeTransformed in the failure case, as it is less useful, and likely to skew metrics
				failureList = append(failureList, failure)
			}
		}
		return models.NewTransformationResult(successList, filteredList, failureList)
	}
}
