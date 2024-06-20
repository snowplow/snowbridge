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

package inmemory

import (
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
	"github.com/twinj/uuid"
)

type Configuration struct{}

type inMemorySource struct {
	messages   chan []string
	log        *log.Entry
	exitSignal chan struct{}
}

func configfunction(messages chan []string) func(c *Configuration) (sourceiface.Source, error) {
	return func(c *Configuration) (sourceiface.Source, error) {
		return newInMemorySource(messages)
	}
}

type adapter func(i interface{}) (interface{}, error)

func (f adapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

func (f adapter) ProvideDefault() (interface{}, error) {
	cfg := &Configuration{}

	return cfg, nil
}

func adapterGenerator(f func(c *Configuration) (sourceiface.Source, error)) adapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Configuration)
		if !ok {
			return nil, errors.New("invalid input")
		}

		return f(cfg)
	}
}

func ConfigPair(messages chan []string) config.ConfigurationPair {
	return config.ConfigurationPair{
		Name:   "inMemory",
		Handle: adapterGenerator(configfunction(messages)),
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
		case msgs := <-ss.messages:
			timeNow := time.Now().UTC()
			var mods []*models.Message
			for _, m := range msgs {
				mod := models.Message{
					Data:         []byte(m),
					PartitionKey: uuid.NewV4().String(),
					TimeCreated:  timeNow,
					TimePulled:   timeNow,
				}
				mods = append(mods, &mod)
			}

			err := sf.WriteToTarget(mods)
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
