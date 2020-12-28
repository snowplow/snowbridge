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
	"sync"
	"time"
)

// StdinSource holds a new client for reading events from stdin
type StdinSource struct {
	log *log.Entry
}

// NewStdinSource creates a new client for reading events from stdin
func NewStdinSource() (*StdinSource, error) {
	return &StdinSource{
		log: log.WithFields(log.Fields{"name": "StdinSource"}),
	}, nil
}

// Read will execute until CTRL + D is pressed or until EOF is passed
func (ss *StdinSource) Read(sf *SourceFunctions) error {
	ss.log.Infof("Reading messages from 'stdin', scanning until EOF detected (Note: Press 'CTRL + D' to exit)")

	// TODO: Make the goroutine count configurable
	throttle := make(chan struct{}, 20)
	wg := sync.WaitGroup{}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		timeNow := time.Now().UTC()
		events := []*Event{
			{
				Data:         []byte(scanner.Text()),
				PartitionKey: uuid.NewV4().String(),
				TimeCreated:  timeNow,
				TimePulled:   timeNow,
			},
		}

		throttle <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := sf.WriteToTarget(events)
			if err != nil {
				ss.log.Error(err)
			}
			<-throttle
		}()
	}
	wg.Wait()

	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
}
