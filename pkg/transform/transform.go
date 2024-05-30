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

package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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
		// if len(tranformFunctions) == 0 {
		// 	return models.NewTransformationResult(messages, filteredList, failureList)
		// }

		for _, message := range messages {
			msg := *message // dereference to avoid amending input
			success := &msg // success must be both input and output to a TransformationFunction, so we make this pointer.
			if success.Meta == nil {
				success.Meta = make(map[string]interface{})
			}
			// Original data as a string for debug logging
			success.Meta["original"] = string(message.Data)
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
				// Transformed stays as a raw json
				if success.Meta == nil {
					success.Meta = make(map[string]interface{})
				}
				success.Meta["transformed"] = json.RawMessage(success.Data)
				success.TimeTransformed = time.Now().UTC()
				successList = append(successList, success)
			}
			// We don't append TimeTransformed in the failure or filtered cases, as it is less useful, and likely to skew metrics
			if filtered != nil {
				filteredList = append(filteredList, filtered)

				// Filtering gets logged.
				if filtered.Meta == nil {
					filtered.Meta = make(map[string]interface{})
				}
				filtered.Meta["filtered"] = true
				metaJSON, err := json.Marshal(filtered.Meta)
				if err != nil {
					fmt.Println("ERROR MARSHALING FILTER META: " + err.Error())
				}
				resp, err := http.Post(os.Getenv("META_HTTP_ADDRESS"), "application/json", bytes.NewBuffer(metaJSON))
				if err != nil {
					fmt.Println("ERROR SENDING FILTER META REQUEST: " + err.Error())
				}

				resp.Body.Close()

			}
			if failure != nil {
				failure.Meta["transformation_error"] = failure.GetError().Error()
				failureList = append(failureList, failure)
			}
		}
		return models.NewTransformationResult(successList, filteredList, failureList)
	}
}
