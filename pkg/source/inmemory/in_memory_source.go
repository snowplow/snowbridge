/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package inmemory

import (
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
	"github.com/twinj/uuid"
)

// ConfigPair is passed to configuration to determine when to build in memory source.
func ConfigPair(messages chan []string) config.ConfigurationPair {
	return config.ConfigurationPair{
		Name:   "inMemory",
		Handle: adapterGenerator(configfunction(messages)),
	}
}

type configuration struct{}

type inMemorySource struct {
	messages   chan []string
	log        *log.Entry
	exitSignal chan struct{}
}

func configfunction(messages chan []string) func(c *configuration) (sourceiface.Source, error) {
	return func(c *configuration) (sourceiface.Source, error) {
		return newInMemorySource(messages)
	}
}

type adapter func(i any) (any, error)

func (f adapter) Create(i any) (any, error) {
	return f(i)
}

func (f adapter) ProvideDefault() (any, error) {
	cfg := &configuration{}

	return cfg, nil
}

func adapterGenerator(f func(c *configuration) (sourceiface.Source, error)) adapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*configuration)
		if !ok {
			return nil, errors.New("invalid input")
		}

		return f(cfg)
	}
}

func newInMemorySource(messages chan []string) (*inMemorySource, error) {
	return &inMemorySource{
		log:        log.WithFields(log.Fields{"source": "in_memory"}),
		messages:   messages,
		exitSignal: make(chan struct{}),
	}, nil
}

func (ss *inMemorySource) Read(sf *sourceiface.SourceFunctions) error {
	ss.log.Infof("Reading messages from in memory buffer")

processing:
	for {
		select {
		case <-ss.exitSignal:
			break processing
		case input := <-ss.messages:
			timeNow := time.Now().UTC()
			var messages []*models.Message
			for _, single := range input {
				message := models.Message{
					Data:         []byte(single),
					PartitionKey: uuid.NewV4().String(),
					TimeCreated:  timeNow,
					TimePulled:   timeNow,
				}
				messages = append(messages, &message)
			}

			err := sf.WriteToTarget(messages)
			if err != nil {
				ss.log.WithFields(log.Fields{"error": err}).Error(err)
			}
		}
	}

	ss.log.Infof("Done with processing")
	return nil
}

func (ss *inMemorySource) Stop() {
	ss.log.Warn("Stopping in memory source")
	ss.exitSignal <- struct{}{}
}

func (ss *inMemorySource) GetID() string {
	return "inMemory"
}
