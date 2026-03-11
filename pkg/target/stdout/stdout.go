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

package stdout

import (
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const SupportedTargetStdout = "stdout"

// StdoutTargetConfig configures the destination for records consumed
type StdoutTargetConfig struct {
	BatchingConfig *targetiface.BatchingConfig `hcl:"batching,block"`
	DataOnlyOutput bool                        `hcl:"data_only_output,optional"`
}

// StdoutTargetDriver holds a new client for writing messages to stdout
type StdoutTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig
	output         io.Writer
	dataOnlyOutput bool

	log *log.Entry
}

// newStdoutTargettWithInterfaces allows you to provide an Stdout directly to allow
// for mocking and localstack usage
func (st *StdoutTargetDriver) initWithInterfaces(writer io.Writer, dataOnlyOutput bool) error {
	st.output = writer
	st.dataOnlyOutput = dataOnlyOutput
	st.log = log.WithFields(log.Fields{"target": SupportedTargetStdout})
	return nil
}

// BuildStdoutFromConfig creates a stdout target from decoded configuration
func (st *StdoutTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*StdoutTargetConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	st.SetBatchingConfig(*cfg.BatchingConfig)
	return st.initWithInterfaces(os.Stdout, cfg.DataOnlyOutput)
}

func (st *StdoutTargetDriver) SetBatchingConfig(batchingConfig targetiface.BatchingConfig) {
	st.BatchingConfig = batchingConfig
}

func (st *StdoutTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return st.BatchingConfig
}

// GetDefaultConfiguration returns the default configuration for stdout target
func (st *StdoutTargetDriver) GetDefaultConfiguration() any {
	return &StdoutTargetConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     1,
			MaxBatchBytes:        1048576,
			MaxMessageBytes:      1048576,
			MaxConcurrentBatches: 1,
			FlushPeriodMillis:    500,
		},
		DataOnlyOutput: false,
	}
}

// ChunkBatches combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (st *StdoutTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, st.BatchingConfig)
}

// Write pushes all messages to the required target
func (st *StdoutTargetDriver) Write(messages []*models.Message) (result *models.TargetWriteResult, err error) {
	st.log.Debugf("Writing %d messages to stdout ...", len(messages))

	var sent []*models.Message

	for _, msg := range messages {
		msg.TimeRequestStarted = time.Now().UTC()
		if st.dataOnlyOutput {
			if _, err := fmt.Fprintln(st.output, string(msg.Data)); err != nil {
				st.log.WithError(err).Error("failed to write into stdout")
			}
		} else {
			if _, err := fmt.Fprintln(st.output, msg.String()); err != nil {
				st.log.WithError(err).Error("failed to write into stdout")
			}
		}
		msg.TimeRequestFinished = time.Now().UTC()

		if msg.AckFunc != nil {
			msg.AckFunc()
		}

		sent = append(sent, msg)
	}

	return models.NewTargetWriteResult(sent, nil, nil), nil
}

// Open does not do anything for this target
func (st *StdoutTargetDriver) Open() error {
	return nil
}

// Close does not do anything for this target
func (st *StdoutTargetDriver) Close() {}
