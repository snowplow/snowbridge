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

package silent

import (
	"fmt"
	"time"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const SupportedTargetSilent = "silent"

// SilentTargetConfig contains configurable options for the silent target
type SilentTargetConfig struct {
	BatchingConfig *targetiface.BatchingConfig `hcl:"batching,block"`
}

// SilentTargetDriver holds a new client for silently acking data
type SilentTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig
}

// GetDefaultConfiguration returns the default configuration for Silent target
func (st *SilentTargetDriver) GetDefaultConfiguration() any {
	return &SilentTargetConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     1,
			MaxBatchBytes:        100000000000,
			MaxMessageBytes:      100000000000,
			MaxConcurrentBatches: 1,
			FlushPeriodMillis:    1,
		},
	}
}

func (st *SilentTargetDriver) SetBatchingConfig(batchingConfig targetiface.BatchingConfig) {
	st.BatchingConfig = batchingConfig
}

func (st *SilentTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return st.BatchingConfig
}

// InitFromConfig creates a Silent target from decoded configuration
func (st *SilentTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*SilentTargetConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	// Set the batching config
	st.SetBatchingConfig(*cfg.BatchingConfig)

	return nil
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (st *SilentTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, st.BatchingConfig)
}

// Write pushes all messages to the required target
// It's just acking data, nothing more
func (st *SilentTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	now := time.Now().UTC()
	for _, msg := range messages {
		msg.TimeRequestStarted = now
		msg.TimeRequestFinished = now

		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	return models.NewTargetWriteResult(
		messages,
		nil,
		nil,
	), nil
}

// Open does not do anything for this target
func (st *SilentTargetDriver) Open() error {
	return nil
}

// Close does not do anything for this target
func (st *SilentTargetDriver) Close() {}
