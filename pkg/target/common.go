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

package target

import (
	"github.com/snowplow/snowbridge/pkg/batchtransform"
	"github.com/snowplow/snowbridge/pkg/models"
)

// chunkBatcherWithConfig returns a batch transformation which incorporates models.GetChunkedMessages() into the batch transformation model.
// It is done this way in order to pass GetChunkedMessages its config within the confines of the BatchTransfomration design
func chunkBatcherWithConfig(chunkSize int, maxMessageByteSize int, maxChunkByteSize int) batchtransform.BatchTransformationFunction {

	// chunkBatcher is a batch transformation which incorporates models.GetChunkedMessages() into the batch transformation model,
	// preserving the original logic and ownership of the function.
	chunkBatcher := func(batchesIn []*models.MessageBatch) ([]*models.MessageBatch, []*models.Message, []*models.Message) {
		oversizedOut := make([]*models.Message, 0)
		chunkedBatches := make([]*models.MessageBatch, 0)

		for _, batch := range batchesIn {
			chunks, oversized := models.GetChunkedMessages(batch.OriginalMessages, chunkSize, maxMessageByteSize, maxChunkByteSize)

			oversizedOut = append(oversizedOut, oversized...)

			for _, chunk := range chunks {
				asBatch := &models.MessageBatch{
					OriginalMessages: chunk,
				}

				chunkedBatches = append(chunkedBatches, asBatch)
			}

		}
		return chunkedBatches, nil, oversizedOut
	}

	return chunkBatcher
}
