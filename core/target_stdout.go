// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	log "github.com/sirupsen/logrus"
)

// StdoutTarget holds a new client for writing events to stdout
type StdoutTarget struct{}

// NewStdoutTarget creates a new client for writing events to stdout
func NewStdoutTarget() *StdoutTarget {
	return &StdoutTarget{}
}

// Write pushes all events to the required target
func (st *StdoutTarget) Write(events []*Event) error {
	for _, event := range events {
		data := string(event.Data)
		log.Infof("PartitionKey: %s, Data: %s\n", event.PartitionKey, data)
	}
	return nil
}
