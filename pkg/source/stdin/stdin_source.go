// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package stdinsource

import (
	"bufio"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// stdinSource holds a new client for reading messages from stdin
type stdinSource struct {
	concurrentWrites int

	log *log.Entry
}

// configfunction returns an stdin source from a config
func configfunction(c *config.Config) (sourceiface.Source, error) {
	return newStdinSource(
		c.Sources.ConcurrentWrites,
	)
}

// ConfigPair is passed to configuration to determine when to build an stdin source.
var ConfigPair = sourceconfig.ConfigPair{SourceName: "stdin", SourceConfigFunc: configfunction}

// newStdinSource creates a new client for reading messages from stdin
func newStdinSource(concurrentWrites int) (*stdinSource, error) {
	return &stdinSource{
		concurrentWrites: concurrentWrites,
		log:              log.WithFields(log.Fields{"source": "stdin"}),
	}, nil
}

// Read will execute until CTRL + D is pressed or until EOF is passed
func (ss *stdinSource) Read(sf *sourceiface.SourceFunctions) error {
	ss.log.Infof("Reading messages from 'stdin', scanning until EOF detected (Note: Press 'CTRL + D' to exit)")

	throttle := make(chan struct{}, ss.concurrentWrites)
	wg := sync.WaitGroup{}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		timeNow := time.Now().UTC()
		messages := []*models.Message{
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
			err := sf.WriteToTarget(messages)
			if err != nil {
				ss.log.WithFields(log.Fields{"error": err}).Error(err)
			}
			<-throttle
		}()
	}
	wg.Wait()

	if scanner.Err() != nil {
		return errors.Wrap(scanner.Err(), "Failed to read from stdin scanner")
	}
	return nil
}

// Stop will halt the reader processing more events
func (ss *stdinSource) Stop() {
	ss.log.Warn("Press CTRL + D to exit!")
}

// GetID returns the identifier for this source
func (ss *stdinSource) GetID() string {
	return "stdin"
}
