/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package batchtransform

import "github.com/snowplow/snowbridge/pkg/models"

// BatchTransformationFunction is a transformation function which operates across a batch of events
// It takes a batch as an input, and returns a successful batch and a slice of invalid messages
type BatchTransformationFunction func([]models.MessageBatch) (success []models.MessageBatch, invalid []*models.Message, oversized []*models.Message)

// BatchTransformationApplyFunction combines batch into one callable function
type BatchTransformationApplyFunction func([]*models.Message, []BatchTransformationFunction, []BatchTransformationFunction) models.BatchTransformationResult

// BatchTransformationGenerator returns a BatchTransformationApplyFunction from a provided set of BatchTransformationFunctions
type BatchTransformationGenerator func(...BatchTransformationFunction) BatchTransformationApplyFunction

// NewBatchTransformation constructs a function which applies all transformations to all messages, returning a TransformationResult.
func NewBatchTransformation(tranformFunctions ...BatchTransformationFunction) BatchTransformationApplyFunction {
	// pre is a function to be run before the configured ones, post is to be run after.
	// This is done because sometimes functions need to _always_ run first or last, depending on the specific target logic. (eg. batching by dynamic headers, if configured)
	// pre and post functions are intended for use only in the implementations of targets.
	return func(messages []*models.Message, pre []BatchTransformationFunction, post []BatchTransformationFunction) models.BatchTransformationResult {
		// make a batch to begin with
		success := []models.MessageBatch{{OriginalMessages: messages}}

		// Because http will require specific functions to always go first and last, we provide these here
		// Compiler gets confused if we don't rename.
		functionsToRun := append(pre, tranformFunctions...)
		functionsToRun = append(functionsToRun, post...)

		// If no transformations, just return a result
		if len(functionsToRun) == 0 {
			return models.BatchTransformationResult{Success: success}
		}

		var invalid []*models.Message
		var oversized []*models.Message
		invalidList := make([]*models.Message, 0, len(messages))
		oversizedList := make([]*models.Message, 0, len(messages))
		// Run each transformation
		for _, transformFunction := range functionsToRun {
			// success is recomputed each time into a complete list of batches
			success, invalid, oversized = transformFunction(success)
			// Invalids are excluded each iteration so must be appended to a permanent list
			invalidList = append(invalidList, invalid...)

			oversizedList = append(oversizedList, oversized...)
		}

		return models.BatchTransformationResult{Success: success, Invalid: invalidList, Oversized: oversizedList}
	}
}
