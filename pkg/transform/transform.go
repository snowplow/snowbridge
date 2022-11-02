// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"time"

	"github.com/snowplow/snowbridge/pkg/models"
)

// TransformationFunction takes a message and intermediateState, and returns a transformed message, a filtered message or an errored message, along with an intermediateState
type TransformationFunction func(*models.Message, interface{}) (*models.Message, *models.Message, *models.Message, interface{})

// TransformationApplyFunction dereferences messages before running transformations, and returns a TransformationResult
type TransformationApplyFunction func([]*models.Message) *models.TransformationResult

// TransformationGenerator returns a TransformationApplyFunction from a provided set of TransformationFunctions
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
			// We don't append TimeTransformed in the failure or filtered cases, as it is less useful, and likely to skew metrics
			if filtered != nil {
				filteredList = append(filteredList, filtered)
			}
			if failure != nil {
				failureList = append(failureList, failure)
			}
		}
		return models.NewTransformationResult(successList, filteredList, failureList)
	}
}
