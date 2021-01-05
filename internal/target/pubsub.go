// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"

	"github.com/snowplow-devops/stream-replicator/internal/common"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

const (
	// API Documentation: https://cloud.google.com/pubsub/quotas

	// Each record can only be up to 10 MiB in size
	pubSubPublishMessageByteLimit = 10485760
)

// PubSubTarget holds a new client for writing messages to Google PubSub
type PubSubTarget struct {
	projectID string
	client    *pubsub.Client
	topic     *pubsub.Topic
	topicName string

	log *log.Entry
}

// PubSubPublishResult contains the publish result and the function to execute
// on success to ack the send
type PubSubPublishResult struct {
	Result  *pubsub.PublishResult
	AckFunc func()
}

// NewPubSubTarget creates a new client for writing messages to Google PubSub
func NewPubSubTarget(projectID string, topicName string, serviceAccountB64 string) (*PubSubTarget, error) {
	if serviceAccountB64 != "" {
		targetFile, err := common.GetGCPServiceAccountFromBase64(serviceAccountB64)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to store GCP Service Account JSON file")
		}
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", targetFile)
	}

	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create PubSub client")
	}

	return &PubSubTarget{
		projectID: projectID,
		client:    client,
		topicName: topicName,
		log:       log.WithFields(log.Fields{"target": "pubsub", "cloud": "GCP", "project": projectID, "topic": topicName}),
	}, nil
}

// Write pushes all messages to the required target
func (ps *PubSubTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	messageCount := int64(len(messages))
	ps.log.Debugf("Writing %d messages to topic ...", messageCount)

	ctx := context.Background()

	if ps.topic == nil {
		err := errors.New("Topic has not been opened, must call Open() before attempting to write")

		sent := int64(0)
		failed := messageCount

		return models.NewTargetWriteResult(
			sent,
			failed,
			messages,
			nil,
		), err
	}

	var results []*PubSubPublishResult

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		ps.MaximumAllowedMessageSizeBytes(),
	)

	for _, msg := range safeMessages {
		pubSubMsg := &pubsub.Message{
			Data: msg.Data,
		}

		r := ps.topic.Publish(ctx, pubSubMsg)
		results = append(results, &PubSubPublishResult{
			Result:  r,
			AckFunc: msg.AckFunc,
		})
	}

	sent := int64(0)
	failed := int64(0)

	var errResult error

	for _, r := range results {
		_, err := r.Result.Get(ctx)

		if err != nil {
			errResult = multierror.Append(errResult, err)
			failed++
		} else {
			sent++
			if r.AckFunc != nil {
				r.AckFunc()
			}
		}
	}

	if errResult != nil {
		errResult = errors.Wrap(errResult, "Error writing messages to PubSub topic")
	}

	ps.log.Debugf("Successfully wrote %d/%d messages", sent, len(safeMessages))
	return models.NewTargetWriteResult(
		sent,
		failed,
		safeMessages,
		oversized,
	), errResult
}

// Open opens a pipe to the topic
func (ps *PubSubTarget) Open() {
	ps.log.Warnf("Opening target for topic '%s' in project %s", ps.topicName, ps.projectID)
	ps.topic = ps.client.Topic(ps.topicName)
}

// Close stops the topic
func (ps *PubSubTarget) Close() {
	ps.log.Warnf("Closing target for topic '%s' in project %s", ps.topicName, ps.projectID)
	ps.topic.Stop()
	ps.topic = nil
}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
func (ps *PubSubTarget) MaximumAllowedMessageSizeBytes() int {
	return pubSubPublishMessageByteLimit
}
