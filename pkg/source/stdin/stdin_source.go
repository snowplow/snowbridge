/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package stdinsource

import (
	"bufio"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// Configuration configures the source for records pulled
type Configuration struct {
	ConcurrentWrites int `hcl:"concurrent_writes,optional"`
}

// stdinSource holds a new client for reading messages from stdin
type stdinSource struct {
	concurrentWrites int

	log *log.Entry
}

// configFunction returns an stdin source from a config
func configfunction(c *Configuration) (sourceiface.Source, error) {
	return newStdinSource(
		c.ConcurrentWrites,
	)
}

// The adapter type is an adapter for functions to be used as
// pluggable components for Stdin Source. It implements the Pluggable interface.
type adapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f adapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &Configuration{
		ConcurrentWrites: 50,
	}

	return cfg, nil
}

// adapterGenerator returns a StdinSource adapter.
func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input, expected StdinSourceConfig")
		}

		return f(cfg)
	}
}

// ConfigPair is passed to configuration to determine when to build an stdin source.
var ConfigPair = config.ConfigurationPair{
	Name:   "stdin",
	Handle: adapterGenerator(configfunction),
}

// newStdinSource creates a new client for reading messages from stdin
func newStdinSource(concurrentWrites int) (*stdinSource, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()
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
				PartitionKey: uuid.New().String(),
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
