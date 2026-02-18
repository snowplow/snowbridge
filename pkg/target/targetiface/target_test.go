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

package targetiface

import (
	"testing"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestDefaultBatcher_EmptyCurrentBatch_SingleMessage(t *testing.T) {
	assert := assert.New(t)

	config := BatchingConfig{
		MaxBatchMessages: 10,
		MaxBatchBytes:    1000,
		MaxMessageBytes:  500,
	}

	currentBatch := CurrentBatch{
		Messages:  []*models.Message{},
		DataBytes: 0,
	}

	message := models.Message{Data: []byte("hello")} // 5 bytes

	batchesToSend, newCurrentBatch, oversized := DefaultBatcher(currentBatch, &message, config)

	assert.Empty(batchesToSend, "Should not send batch with only one message")
	assert.Equal(1, len(newCurrentBatch.Messages))
	assert.Equal(5, newCurrentBatch.DataBytes)
	assert.Empty(oversized)
}

func TestDefaultBatcher_FillsToMaxMessages(t *testing.T) {
	assert := assert.New(t)

	config := BatchingConfig{
		MaxBatchMessages: 3,
		MaxBatchBytes:    1000,
		MaxMessageBytes:  500,
	}

	// Create a current batch with 2 messages (one less than max)
	currentBatch := CurrentBatch{
		Messages: []*models.Message{
			{Data: []byte("msg1")}, // 4 bytes
			{Data: []byte("msg2")}, // 4 bytes
		},
		DataBytes: 8,
	}

	// Add one more message (the 3rd) to trigger batch send
	additionalMessage := &models.Message{Data: []byte("msg3")} // 4 bytes

	batchToSend, newCurrentBatch, oversized := DefaultBatcher(currentBatch, additionalMessage, config)

	// Verify complete batch is sent (3 messages - the max)
	assert.Equal(3, len(batchToSend), "Should send complete batch of 3 messages")

	// Verify new current batch is empty
	assert.Equal(0, len(newCurrentBatch.Messages), "Should have empty current batch after sending")
	assert.Equal(0, newCurrentBatch.DataBytes, "Should have 0 bytes in new current batch")

	// Verify no oversized message
	assert.Nil(oversized, "Should have no oversized message")
}

func TestDefaultBatcher_FillsToMaxBytes(t *testing.T) {
	assert := assert.New(t)

	config := BatchingConfig{
		MaxBatchMessages: 10,
		MaxBatchBytes:    100,
		MaxMessageBytes:  500,
	}

	// Create a current batch with 2 messages totaling 80 bytes (close to max of 100)
	currentBatch := CurrentBatch{
		Messages: []*models.Message{
			{Data: make([]byte, 40)}, // 40 bytes
			{Data: make([]byte, 40)}, // 40 bytes
		},
		DataBytes: 80,
	}

	// Add one more message that would exceed max bytes, triggering batch send
	additionalMessage := &models.Message{Data: make([]byte, 40)} // 40 bytes - would make 120 total

	batchToSend, newCurrentBatch, oversized := DefaultBatcher(currentBatch, additionalMessage, config)

	// Verify batch is sent (2 messages totaling 80 bytes)
	assert.Equal(2, len(batchToSend), "Should send batch of 2 messages")

	// Verify new current batch contains the additional message
	assert.Equal(1, len(newCurrentBatch.Messages), "Should have 1 message in new current batch")
	assert.Equal(40, newCurrentBatch.DataBytes, "Should have 40 bytes in new current batch")

	// Verify no oversized message
	assert.Nil(oversized, "Should have no oversized message")
}

func TestDefaultBatcher_OversizedMessage(t *testing.T) {
	assert := assert.New(t)

	config := BatchingConfig{
		MaxBatchMessages: 10,
		MaxBatchBytes:    1000,
		MaxMessageBytes:  50,
	}

	currentBatch := CurrentBatch{
		Messages:  []*models.Message{},
		DataBytes: 0,
	}

	// Single oversized message (100 bytes when max is 50)
	oversizedMessage := &models.Message{Data: make([]byte, 100)}

	batchToSend, newCurrentBatch, oversized := DefaultBatcher(currentBatch, oversizedMessage, config)

	// Verify no batch is sent
	assert.Nil(batchToSend, "Should not send any batch for oversized message")

	// Verify current batch remains empty
	assert.Equal(0, len(newCurrentBatch.Messages), "Should have empty current batch")
	assert.Equal(0, newCurrentBatch.DataBytes, "Should have 0 bytes in current batch")

	// Verify oversized message is returned
	assert.NotNil(oversized, "Should return oversized message")
	assert.Equal(oversizedMessage, oversized, "Should return the exact oversized message")
	assert.Equal(100, len(oversized.Data), "Oversized message should have 100 bytes")
}

func TestDefaultBatcher_ContinuesFromCurrentBatch(t *testing.T) {
	assert := assert.New(t)

	config := BatchingConfig{
		MaxBatchMessages: 10,
		MaxBatchBytes:    100, // Low byte limit to trigger send
		MaxMessageBytes:  500,
	}

	// Start with a batch that already has 2 messages totaling 80 bytes (close to limit)
	existingMsg1 := &models.Message{Data: make([]byte, 40)} // 40 bytes
	existingMsg2 := &models.Message{Data: make([]byte, 40)} // 40 bytes
	currentBatch := CurrentBatch{
		Messages: []*models.Message{
			existingMsg1,
			existingMsg2,
		},
		DataBytes: 80,
	}

	// Add a new message that would exceed the byte limit, triggering batch send
	newMsg := &models.Message{Data: make([]byte, 30)} // 30 bytes - would make 110 total, exceeding 100

	batchToSend, newCurrentBatch, oversized := DefaultBatcher(currentBatch, newMsg, config)

	// Verify batch is sent with the existing messages
	assert.Equal(2, len(batchToSend), "Should send batch with 2 existing messages")

	// ANTI-STALENESS: Verify sent batch contains existing messages
	assert.Equal(existingMsg1, batchToSend[0], "Batch should contain existing1 (anti-staleness)")
	assert.Equal(existingMsg2, batchToSend[1], "Batch should contain existing2 (anti-staleness)")

	// ANTI-STALENESS: New current batch should only contain NEW message
	assert.Equal(1, len(newCurrentBatch.Messages), "Should have 1 new message in current batch")
	assert.Equal(newMsg, newCurrentBatch.Messages[0], "New current batch should only contain NEW message (anti-staleness)")
	assert.Equal(30, newCurrentBatch.DataBytes, "New current batch should have 30 bytes")

	// Verify no oversized message
	assert.Nil(oversized, "Should have no oversized message")
}

func TestDefaultBatcher_FinalCheckSendsBatchAtByteCap(t *testing.T) {
	assert := assert.New(t)

	config := BatchingConfig{
		MaxBatchMessages: 10,
		MaxBatchBytes:    100,
		MaxMessageBytes:  500,
	}

	// Test case 2: When we're very close to MaxBatchBytes, the batch should be sent immediately
	msg1 := &models.Message{Data: make([]byte, 50)} // 50 bytes
	msg2 := &models.Message{Data: make([]byte, 45)} // 45 bytes - total 95 bytes (close to 100)

	currentBatch := CurrentBatch{
		Messages:  []*models.Message{msg1},
		DataBytes: len(msg1.Data),
	}

	batchesToSend, newCurrentBatch, oversized := DefaultBatcher(currentBatch, msg2, config)

	// The final check should trigger because we have 95 bytes in current batch
	// and the next message in the batch (which would be msg2 when checking) would exceed 100
	assert.NotNil(batchesToSend, "Should send batch when close to byte limit")
	assert.Equal(2, len(batchesToSend), "Batch should have 2 messages (95 bytes)")
	assert.Equal(msg1, batchesToSend[0], "Batch should contain first message")
	assert.Equal(msg2, batchesToSend[1], "Batch should contain second message")
	assert.Equal(0, len(newCurrentBatch.Messages), "New current batch should be empty")
	assert.Equal(0, newCurrentBatch.DataBytes)
	assert.Empty(oversized)
}
