// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"fmt"
	log "github.com/sirupsen/logrus"
)

// StdoutTarget holds a new client for writing messages to stdout
type StdoutTarget struct {
	log *log.Entry
}

// NewStdoutTarget creates a new client for writing messages to stdout
func NewStdoutTarget() (*StdoutTarget, error) {
	return &StdoutTarget{
		log: log.WithFields(log.Fields{"name": "StdoutTarget"}),
	}, nil
}

// Write pushes all messages to the required target
func (st *StdoutTarget) Write(messages []*Message) (*TargetWriteResult, error) {
	st.log.Debugf("Writing %d messages to stdout ...", len(messages))

	for _, msg := range messages {
		fmt.Println(msg.String())

		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	return NewWriteResult(int64(len(messages)), int64(0), messages), nil
}

// Close does not do anything for this target
func (st *StdoutTarget) Close() {}
