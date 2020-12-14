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
	st.log.Debugf("Writing %d messages to stdout ...", len(messages))

	for _, msg := range messages {
		fmt.Println(msg.String())

		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	return models.NewWriteResult(int64(len(messages)), int64(0), messages), nil
}

// Open does not do anything for this target
func (st *StdoutTarget) Open() {}

// Close does not do anything for this target
func (st *StdoutTarget) Close() {}
