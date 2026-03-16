/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package transform

import (
	"time"

	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// TransformationFunction takes a message and intermediateState, and returns a transformed message, a filtered message or an errored message, along with an intermediateState
type TransformationFunction func(*models.Message, any) (*models.Message, *models.Message, *models.Message, any)

// TransformationApplyFunction applies transformations to a message and returns a TransformationResult
type TransformationApplyFunction func(*models.Message) *models.TransformationResult

// TransformationGenerator returns a TransformationApplyFunction from a provided set of TransformationFunctions
type TransformationGenerator func(...TransformationFunction) TransformationApplyFunction

// NewTransformation constructs a function which applies all transformations to a single message, returning a TransformationResult.
func NewTransformation(transformFunctions ...TransformationFunction) TransformationApplyFunction {
	return func(message *models.Message) *models.TransformationResult {
		var transformedMsg *models.Message
		var filteredMsg *models.Message
		var failureMsg *models.Message

		// Preserve original data before any transformations (needed for failure payloads)
		buffer := make([]byte, len(message.Data))
		copy(buffer, message.Data)
		message.OriginalData = buffer

		// If no transformations, just return the result rather than shuffling data
		if len(transformFunctions) == 0 {
			return models.NewTransformationResult(message, nil, nil)
		}

		message.TimeTransformationStarted = time.Now().UTC()
		transformed := message

		var failure *models.Message
		var filtered *models.Message
		var intermediate any
		for _, transformFunction := range transformFunctions {
			// Overwrite the input for each iteration in sequence of transformations,
			// since the desired result is a single transformed message with a nil failure, or a nil message with a single failure
			transformed, filtered, failure, intermediate = transformFunction(transformed, intermediate)
			if failure != nil || filtered != nil {
				break
			}
		}
		if transformed != nil {
			transformed.TimeTransformed = time.Now().UTC()
			transformedMsg = transformed
		}
		// We don't append TimeTransformed in the failure or filtered cases, as it is less useful, and likely to skew metrics
		if filtered != nil {
			filteredMsg = filtered
		}
		if failure != nil {
			failureMsg = failure
		}

		return models.NewTransformationResult(transformedMsg, filteredMsg, failureMsg)
	}
}
