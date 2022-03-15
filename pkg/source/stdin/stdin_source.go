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

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// StdinSourceConfig configures the source for records pulled
type StdinSourceConfig struct {
	ConcurrentWrites int `hcl:"concurrent_writes,optional" env:"SOURCE_CONCURRENT_WRITES"`
}

// StdinSource holds a new client for reading messages from stdin
type StdinSource struct {
	concurrentWrites int

	log *log.Entry
}

// StdinSourceConfigfunction returns an stdin source from a config
func StdinSourceConfigfunction(c *StdinSourceConfig) (sourceiface.Source, error) {
	return NewStdinSource(
		c.ConcurrentWrites,
	)
}

// The StdinSourceAdapter type is an adapter for functions to be used as
// pluggable components for Stdin Source. It implements the Pluggable interface.
type StdinSourceAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f StdinSourceAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f StdinSourceAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults
	cfg := &StdinSourceConfig{
		ConcurrentWrites: 50,
	}

	return cfg, nil
}

// AdaptStdinSourceFunc returns a StdinSourceAdapter.
func AdaptStdinSourceFunc(f func(c *StdinSourceConfig) (sourceiface.Source, error)) StdinSourceAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*StdinSourceConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StdinSourceConfig")
		}

		return f(cfg)
	}
}

// StdinSourceConfigPair is passed to configuration to determine when to build an stdin source.
var StdinSourceConfigPair = sourceconfig.SourceConfigPair{
	Name:   "stdin",
	Handle: AdaptStdinSourceFunc(StdinSourceConfigfunction),
}

// NewStdinSource creates a new client for reading messages from stdin
func NewStdinSource(concurrentWrites int) (*StdinSource, error) {
	return &StdinSource{
		concurrentWrites: concurrentWrites,
		log:              log.WithFields(log.Fields{"source": "stdin"}),
	}, nil
}

// Read will execute until CTRL + D is pressed or until EOF is passed
func (ss *StdinSource) Read(sf *sourceiface.SourceFunctions) error {
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
func (ss *StdinSource) Stop() {
	ss.log.Warn("Press CTRL + D to exit!")
}

// GetID returns the identifier for this source
func (ss *StdinSource) GetID() string {
	return "stdin"
}
