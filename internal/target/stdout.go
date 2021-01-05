// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"fmt"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/internal/models"
)

// StdoutTarget holds a new client for writing messages to stdout
type StdoutTarget struct {
	log *log.Entry
}

// NewStdoutTarget creates a new client for writing messages to stdout
func NewStdoutTarget() (*StdoutTarget, error) {
	return &StdoutTarget{
		log: log.WithFields(log.Fields{"target": "stdout"}),
	}, nil
}

// Write pushes all messages to the required target
func (st *StdoutTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	messageCount := int64(len(messages))
	st.log.Debugf("Writing %d messages to stdout ...", messageCount)

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		st.MaximumAllowedMessageSizeBytes(),
	)

	sent := int64(0)
	failed := int64(0)

	for _, msg := range safeMessages {
		fmt.Println(msg.String())
		sent++

		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	return models.NewTargetWriteResult(
		sent,
		failed,
		safeMessages,
		oversized,
	), nil
}

// Open does not do anything for this target
func (st *StdoutTarget) Open() {}

// Close does not do anything for this target
func (st *StdoutTarget) Close() {}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
//
// Note: Technically no limit but we are putting in a limit of 10 MiB here
//       to avoid trying to print out huge payloads
func (st *StdoutTarget) MaximumAllowedMessageSizeBytes() int {
	return 10485760
}
