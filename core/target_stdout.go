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

// StdoutTarget holds a new client for writing events to stdout
type StdoutTarget struct {
	log *log.Entry
}

// NewStdoutTarget creates a new client for writing events to stdout
func NewStdoutTarget() (*StdoutTarget, error) {
	return &StdoutTarget{
		log: log.WithFields(log.Fields{"name": "StdoutTarget"}),
	}, nil
}

// Write pushes all events to the required target
func (st *StdoutTarget) Write(events []*Event) (*WriteResult, error) {
	st.log.Debugf("Writing %d messages to stdout ...", len(events))

	for _, event := range events {
		data := string(event.Data)
		fmt.Println(fmt.Sprintf("Data:%s,PartitionKey:%s", data, event.PartitionKey))

		if event.AckFunc != nil {
			event.AckFunc()
		}
	}
	return &WriteResult{
		Sent:   int64(len(events)),
		Failed: int64(0),
	}, nil
}

// Close does not do anything for this target
func (st *StdoutTarget) Close() {}
