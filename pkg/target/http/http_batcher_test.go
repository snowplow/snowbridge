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

package http

import (
	"testing"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
	"github.com/stretchr/testify/assert"
)

func TestHTTPBatcher_DynamicHeadersDisabled(t *testing.T) {
	assert := assert.New(t)

	driver := &HTTPTargetDriver{
		dynamicHeaders: false,
		BatchingConfig: targetiface.BatchingConfig{
			MaxBatchMessages: 5,
			MaxBatchBytes:    1000,
			MaxMessageBytes:  500,
		},
	}

	// Test 1: Adding one message to a batch with 4 messages should trigger send
	// Create a current batch with 4 messages (one less than max of 5)
	currentBatch := targetiface.CurrentBatch{
		Messages: []*models.Message{
			{Data: []byte("msg1"), HTTPHeaders: map[string]string{"h1": "v1"}}, // headers ignored
			{Data: []byte("msg2"), HTTPHeaders: map[string]string{"h2": "v2"}}, // headers ignored
			{Data: []byte("msg3")}, // no headers
			{Data: []byte("msg4")}, // no headers
		},
		DataBytes: 16, // 4 + 4 + 4 + 4
	}

	// Add one more message (the 5th)
	additionalMessage := &models.Message{Data: []byte("msg5")}

	batchToSend, newCurrentBatch, oversized := driver.Batcher(currentBatch, additionalMessage)

	// Verify complete batch is sent (5 messages - the max)
	assert.Len(batchToSend, 5, "Should send complete batch of 5 messages")

	// Verify new current batch is empty
	assert.Len(newCurrentBatch.Messages, 0, "Should have empty current batch after sending")
	assert.Equal(0, newCurrentBatch.DataBytes, "Should have 0 bytes in new current batch")

	// Verify no oversized message
	assert.Nil(oversized, "Should have no oversized message")

	// Test 2: Oversized message should be returned as oversized
	// Create an oversized message (larger than 500 bytes)
	oversizedMessage := &models.Message{Data: make([]byte, 600)}

	// Start with empty batch for oversized test
	emptyBatch := targetiface.CurrentBatch{}

	batchToSend2, newCurrentBatch2, oversized2 := driver.Batcher(emptyBatch, oversizedMessage)

	// Verify no batch is sent
	assert.Nil(batchToSend2, "Should not send any batch for oversized message")

	// Verify current batch remains empty
	assert.Len(newCurrentBatch2.Messages, 0, "Current batch should remain empty")
	assert.Equal(0, newCurrentBatch2.DataBytes, "Current batch bytes should remain 0")

	// Verify oversized message is returned
	assert.NotNil(oversized2, "Should return oversized message")
	assert.Equal(oversizedMessage, oversized2, "Should return the exact oversized message")
}

func TestHTTPBatcher_DynamicHeadersEnabled(t *testing.T) {
	assert := assert.New(t)

	driver := &HTTPTargetDriver{
		dynamicHeaders: true,
		BatchingConfig: targetiface.BatchingConfig{
			MaxBatchMessages: 5,
			MaxBatchBytes:    1000,
			MaxMessageBytes:  500,
		},
	}

	// Create a current batch with 4 messages (one less than max, not close to byte limit)
	existingMsg1 := &models.Message{Data: []byte("existing1")} // 9 bytes
	existingMsg2 := &models.Message{Data: []byte("existing2")} // 9 bytes
	existingMsg3 := &models.Message{Data: []byte("existing3")} // 9 bytes
	existingMsg4 := &models.Message{Data: []byte("existing4")} // 9 bytes

	currentBatch := targetiface.CurrentBatch{
		Messages: []*models.Message{
			existingMsg1,
			existingMsg2,
			existingMsg3,
			existingMsg4,
		},
		DataBytes: 36, // 9 + 9 + 9 + 9
	}

	// Add a message with dynamic headers
	messageWithHeaders := &models.Message{
		Data:        []byte("headered"),
		HTTPHeaders: map[string]string{"h1": "v1"},
	}

	batchToSend, newCurrentBatch, oversized := driver.Batcher(currentBatch, messageWithHeaders)

	// Verify message with headers is sent immediately as its own batch
	assert.Equal(1, len(batchToSend), "Should send batch with just the headered message")
	assert.Equal(messageWithHeaders, batchToSend[0], "Batch should contain only the new headered message")

	// Verify current batch is unchanged
	assert.Equal(4, len(newCurrentBatch.Messages), "Current batch should still have 4 messages")
	assert.Equal(36, newCurrentBatch.DataBytes, "Current batch should still have 36 bytes")
	assert.Equal(existingMsg1, newCurrentBatch.Messages[0], "Current batch should contain existing1")
	assert.Equal(existingMsg2, newCurrentBatch.Messages[1], "Current batch should contain existing2")
	assert.Equal(existingMsg3, newCurrentBatch.Messages[2], "Current batch should contain existing3")
	assert.Equal(existingMsg4, newCurrentBatch.Messages[3], "Current batch should contain existing4")

	// Verify no oversized message
	assert.Nil(oversized, "Should have no oversized message")
}
