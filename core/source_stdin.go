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
)

// StdinSource holds a new client for reading events from stdin
type StdinSource struct{}

// NewStdinSource creates a new client for reading events from stdin
func NewStdinSource() (*StdinSource, error) {
	return &StdinSource{}, nil
}

// Read will execute until CTRL + D is pressed or until EOF is passed
func (ss *StdinSource) Read(sf *SourceFunctions) error {
	log.Infof("Reading records from 'stdin', scanning until EOF detected (Note: Press 'CTRL + D' to exit)")

	throttle := make(chan struct{}, 10)
	wg := sync.WaitGroup{}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		events := []*Event{
			{
				Data:         []byte(scanner.Text()),
				PartitionKey: uuid.NewV4().String(),
			},
		}

		throttle <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := sf.WriteToTarget(events)
			if err != nil {
				log.Error(err)
			}
			<-throttle
		}()
	}
	wg.Wait()
	sf.CloseTarget()

	if scanner.Err() != nil {
		return scanner.Err()
	}
	return nil
}
