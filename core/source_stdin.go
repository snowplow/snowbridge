// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"bufio"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"os"
)

// StdinSource holds a new client for reading events from stdin
type StdinSource struct{}

// StdinSource creates a new client for reading events from stdin
func NewStdinSource() (*StdinSource, error) {
	return &StdinSource{}, nil
}

// Read will buffer all inputs until CTRL+D is pressed
func (ss *StdinSource) Read() ([]*Event, error) {
	log.Info("Reading input from 'stdin' ...")

	var events []*Event

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		events = append(events, &Event{
			Data:         []byte(scanner.Text()),
			PartitionKey: uuid.NewV4().String(),
		})
	}

	log.Infof("CTRL+D pressed, returning buffer of '%d' rows", len(events))

	if scanner.Err() != nil {
		log.Error(scanner.Err())
		return nil, scanner.Err()
	}

	return events, nil
}
