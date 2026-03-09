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

package pubsub

import (
	"context"
	"fmt"
	"time"

	// nolint: staticcheck
	"cloud.google.com/go/pubsub"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

const (
	// API Documentation: https://cloud.google.com/pubsub/quotas

	// Each record can only be up to 10 MiB in size
	pubSubPublishMessageByteLimit = 10485760

	SupportedTargetPubsub = "pubsub"
)

// PubSubTargetConfig configures the destination for records consumed
type PubSubTargetConfig struct {
	BatchingConfig  *targetiface.BatchingConfig `hcl:"batching,block"`
	ProjectID       string                      `hcl:"project_id"`
	TopicName       string                      `hcl:"topic_name"`
	CredentialsPath string                      `hcl:"credentials_path,optional"`
}

// PubSubTargetDriver holds a new client for writing messages to Google PubSub
type PubSubTargetDriver struct {
	BatchingConfig targetiface.BatchingConfig
	projectID      string
	client         *pubsub.Client
	topic          *pubsub.Topic
	topicName      string

	log *log.Entry
}

// pubSubPublishResult contains the publish result and the function to execute
// on success to ack the send
type pubSubPublishResult struct {
	Result  *pubsub.PublishResult
	Message *models.Message
}

// GetDefaultConfiguration returns the default configuration for PubSub target
func (ps *PubSubTargetDriver) GetDefaultConfiguration() any {
	return &PubSubTargetConfig{
		BatchingConfig: &targetiface.BatchingConfig{
			MaxBatchMessages:     100,
			MaxBatchBytes:        10485760,
			MaxMessageBytes:      pubSubPublishMessageByteLimit,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    500,
		},
	}
}

func (ps *PubSubTargetDriver) GetBatchingConfig() targetiface.BatchingConfig {
	return ps.BatchingConfig
}

// InitFromConfig initializes the PubSub target driver from configuration
func (ps *PubSubTargetDriver) InitFromConfig(c any) error {
	cfg, ok := c.(*PubSubTargetConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type")
	}

	ps.BatchingConfig = *cfg.BatchingConfig

	ctx := context.Background()

	// Build client options based on provided credentials
	var opts []option.ClientOption

	if cfg.CredentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsPath))
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return errors.Wrap(err, "Failed to create PubSub client")
	}

	ps.projectID = cfg.ProjectID
	ps.client = client
	ps.topicName = cfg.TopicName
	ps.log = log.WithFields(log.Fields{"target": SupportedTargetPubsub, "cloud": "GCP", "project": cfg.ProjectID, "topic": cfg.TopicName})

	return nil
}

// Batcher combines new data with current batch and returns batches ready to send, new current batch, and oversized messages
func (ps *PubSubTargetDriver) Batcher(currentBatch targetiface.CurrentBatch, message *models.Message) (batchToSend []*models.Message, newCurrentBatch targetiface.CurrentBatch, oversized *models.Message) {
	return targetiface.DefaultBatcher(currentBatch, message, ps.BatchingConfig)
}

// Write pushes all messages to the required target
func (ps *PubSubTargetDriver) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	ps.log.Debugf("Writing %d messages to topic ...", len(messages))

	ctx := context.Background()

	if ps.topic == nil {
		err := errors.New("Topic has not been opened, must call Open() before attempting to write")
		failed := messages

		return models.NewTargetWriteResult(
			nil,
			failed,
			nil,
			nil,
		), models.FatalWriteError{Err: err}
	}

	var results []*pubSubPublishResult
	var invalid []*models.Message

	for _, msg := range messages {
		// Sent empty messages to invalid queue
		if len(msg.Data) == 0 {
			msg.SetError(errors.New("pubsub cannot accept empty messages: each message must contain either non-empty data, or at least one attribute"))
			invalid = append(invalid, msg)
			continue
		}

		pubSubMsg := &pubsub.Message{
			Data: msg.Data,
		}
		requestStarted := time.Now().UTC()
		r := ps.topic.Publish(ctx, pubSubMsg)

		msg.TimeRequestStarted = requestStarted

		results = append(results, &pubSubPublishResult{
			Result:  r,
			Message: msg,
		})
	}

	// Manual flush of underlying pubsub buffer
	ps.topic.Flush()

	var sent []*models.Message
	var failed []*models.Message
	var errResult error

	for _, r := range results {
		_, err := r.Result.Get(ctx)

		requestFinished := time.Now().UTC()
		r.Message.TimeRequestFinished = requestFinished

		if err != nil {
			errResult = multierror.Append(errResult, err)

			failed = append(failed, r.Message)
		} else {
			if r.Message.AckFunc != nil {
				r.Message.AckFunc()
			}

			sent = append(sent, r.Message)
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to PubSub topic")
	}

	ps.log.Debugf("Successfully wrote %d/%d messages", len(sent), len(messages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		nil,
		invalid,
	), errResult
}

// Open opens a pipe to the topic
func (ps *PubSubTargetDriver) Open() error {
	ps.log.Warnf("Opening target for topic '%s' in project %s", ps.topicName, ps.projectID)
	ps.topic = ps.client.Topic(ps.topicName)

	ps.topic.PublishSettings.CountThreshold = ps.BatchingConfig.MaxBatchMessages
	ps.topic.PublishSettings.ByteThreshold = ps.BatchingConfig.MaxBatchBytes
	ps.topic.PublishSettings.DelayThreshold = time.Duration(ps.BatchingConfig.FlushPeriodMillis) * time.Millisecond
	return nil
}

// Close stops the topic
func (ps *PubSubTargetDriver) Close() {
	ps.log.Warnf("Closing target for topic '%s' in project %s", ps.topicName, ps.projectID)
	if ps.topic != nil {
		ps.topic.Stop()
		ps.topic = nil
	}
	if ps.client != nil {
		if err := ps.client.Close(); err != nil {
			ps.log.WithError(err).Errorf("error when closing PubSub client")
		}
	}
}
