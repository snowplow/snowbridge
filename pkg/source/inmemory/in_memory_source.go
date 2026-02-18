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
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
	"github.com/twinj/uuid"
)

const SupportedSourceInMemory = "inMemory"

// Build creates an in-memory source from input message channel
func Build(input <-chan []string) (sourceiface.Source, error) {
	driver := &inMemorySourceDriver{
		messages: input,
		log:      log.WithFields(log.Fields{"source": SupportedSourceInMemory}),
	}

	return driver, nil
}

// inMemorySourceDriver holds a client for reading messages from an in-memory channel
type inMemorySourceDriver struct {
	sourceiface.SourceChannels

	messages <-chan []string
	log      *log.Entry
}

// Start will read messages from the in-memory channel and process them
func (ss *inMemorySourceDriver) Start(ctx context.Context) {
	defer close(ss.MessageChannel)
	ss.log.Info("Reading messages from in memory buffer...")

	for {
		select {
		case <-ctx.Done():
			ss.log.Info("Context cancelled, stopping in-memory source")
			return
		case input, ok := <-ss.messages:
			if !ok {
				ss.log.Info("Input channel closed, stopping in-memory source")
				return
			}
			timeNow := time.Now().UTC()
			for _, single := range input {
				message := &models.Message{
					Data:         []byte(single),
					PartitionKey: uuid.NewV4().String(),
					TimeCreated:  timeNow,
					TimePulled:   timeNow,
				}

				select {
				case <-ctx.Done():
					return
				case ss.MessageChannel <- message:
				}
			}
		}
	}
}
