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
	"sync"
	"time"

	"github.com/snowplow/snowbridge/v3/pkg/models"
)

type CurrentBatch struct {
	Messages  []*models.Message
	DataBytes int
}

// TargetDriver describes the interface for batching and writing data to a target
type TargetDriver interface {
	// GetDefaultConfiguration returns the default configuration for the target driver.
	// The default configuraiton must include a BatchingConfig.
	// The function must return a pointer to a concrete struct
	GetDefaultConfiguration() any

	// GetBatchingConfig returns the batching config
	GetBatchingConfig() BatchingConfig

	// InitFromConfig initializes the target driver from a configuration
	InitFromConfig(config any) error

	// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
	Batcher(currentBatch CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch CurrentBatch, oversized *models.Message)

	// Write sends a batch of messages and returns sent, failed, and invalid messages
	Write(messages []*models.Message) (result *models.TargetWriteResult, err error)

	Open() error
	Close()
}

// Batching Config must be attached to the target driver config, and defaults per target managed via the DefaultConfiguration method.
type BatchingConfig struct {
	MaxBatchMessages     int `hcl:"max_batch_messages,optional"`
	MaxBatchBytes        int `hcl:"max_batch_bytes,optional"`
	MaxMessageBytes      int `hcl:"max_message_bytes,optional"`
	MaxConcurrentBatches int `hcl:"max_concurrent_batches,optional"`
	FlushPeriodMillis    int `hcl:"flush_period_millis,optional"`
}

type Target struct {
	TargetDriver

	// Runtime state (managed by Router)
	CurrentBatch CurrentBatch
	Throttle     chan struct{}
	WaitGroup    *sync.WaitGroup
	Ticker       *time.Ticker
	TickerPeriod time.Duration
}

// AddMessages adds messages to the current batch and returns batches ready to send and oversized messages
func (t *Target) AddMessage(message *models.Message) (batchToSend []*models.Message, oversized *models.Message) {
	batchToSend, newCurrentBatch, oversized := t.Batcher(t.CurrentBatch, message)
	t.CurrentBatch = newCurrentBatch
	return batchToSend, oversized
}

// Flush returns the current batch and resets it
func (t *Target) Flush() []*models.Message {
	if len(t.CurrentBatch.Messages) == 0 {
		return nil
	}

	messages := t.CurrentBatch.Messages
	t.CurrentBatch = CurrentBatch{Messages: []*models.Message{}, DataBytes: 0}
	return messages
}

// SpawnThrottledAsyncWrite executes a write function with throttling and wait group management
func (t *Target) SpawnThrottledAsyncWrite(write func()) {
	// Acquire throttle
	t.Throttle <- struct{}{}
	t.WaitGroup.Add(1)

	// Reset the ticker when we call send
	t.Ticker.Reset(t.TickerPeriod)

	go func() {
		defer func() {
			t.WaitGroup.Done()
			<-t.Throttle
		}()

		write()
	}()
}

// Most targets will share the same logic for batching, so we can define a default here for shared use.
// This can be called in a Driver's ChunkBatches function
func DefaultBatcher(currentBatch CurrentBatch, message *models.Message, batchingConfig BatchingConfig) (batchToSend []*models.Message, newCurrentBatch CurrentBatch, oversized *models.Message) {

	msgByteLen := len(message.Data)

	// Check for oversized first.
	if msgByteLen > batchingConfig.MaxMessageBytes {
		return nil, currentBatch, message

		// If our new message takes this batch over the byte limit, schedule the batch for a send, and start a new batch.
	} else if currentBatch.DataBytes > 0 && currentBatch.DataBytes+msgByteLen > batchingConfig.MaxBatchBytes {
		return currentBatch.Messages, CurrentBatch{Messages: []*models.Message{message}, DataBytes: msgByteLen}, nil

		// If the current batch is already at the message count limit, send it and start a new batch with this message.
	} else if len(currentBatch.Messages) == batchingConfig.MaxBatchMessages {
		return currentBatch.Messages, CurrentBatch{Messages: []*models.Message{message}, DataBytes: msgByteLen}, nil

		// Otherwise, append our message to the current batch
	} else {
		currentBatch.Messages = append(currentBatch.Messages, message)
		currentBatch.DataBytes += msgByteLen
	}

	// If the current batch is full after adding new message, return it for sending now and make a new empty batch
	// Full because it's at the max message count,
	if len(currentBatch.Messages) == batchingConfig.MaxBatchMessages ||
		// or full because one more average message in the batch would exceed the byte limit
		(batchingConfig.MaxBatchBytes-currentBatch.DataBytes) < (currentBatch.DataBytes/len(currentBatch.Messages)) {

		return currentBatch.Messages, CurrentBatch{Messages: []*models.Message{}, DataBytes: 0}, nil
	}

	// Otherwise, return with no sends.
	return nil, currentBatch, nil
}
