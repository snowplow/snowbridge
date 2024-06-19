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

package targetiface

import (
	"github.com/hashicorp/go-multierror"
	"github.com/snowplow/snowbridge/pkg/models"
	batchtransform "github.com/snowplow/snowbridge/pkg/transform/batch"
)

// Target describes the interface for how to push the data pulled from the source
type Target interface {
	Write(messages []*models.Message, batchTransformFunc batchtransform.BatchTransformationApplyFunction) (*models.TargetWriteResult, error)
	Open()
	Close()
	MaximumAllowedMessageSizeBytes() int
	GetID() string
}

// TargetProcessFunc defines the API for each target's implementation to handle sending a batch of data.
type TargetProcessFunc func(*models.MessageBatch) (*models.TargetWriteResult, error)

// TargetStruct is an experiment
type TargetStruct struct {
	PrependBatchTransforms []batchtransform.BatchTransformationFunction
	AppendBatchTransforms  []batchtransform.BatchTransformationFunction
	Process                TargetProcessFunc
	// Process should handle acking and retries!
}

func (tgt *TargetStruct) Write(messages []*models.Message, batchTransformFunc batchtransform.BatchTransformationApplyFunction) (*models.TargetWriteResult, error) {

	var errResult error

	// Run the transformations
	batchTransformRes := batchTransformFunc(messages, tgt.PrependBatchTransforms, tgt.AppendBatchTransforms)

	writeResult := &models.TargetWriteResult{
		Oversized: batchTransformRes.Oversized,
		Invalid:   batchTransformRes.Invalid,
	}

	for _, batch := range batchTransformRes.Success {

		res, err := tgt.Process(batch)
		if err != nil {
			// If we have errors, wrap them together to be returned
			errResult = multierror.Append(errResult, err)
			// TODO: Does this make any real sense any more?
		}

		// collate results
		writeResult = writeResult.Append(res)
	}

	return writeResult, errResult
}
