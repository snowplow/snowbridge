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

package stdinsource

import (
	"bufio"
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

const SupportedSourceStdin = "stdin"

// Configuration configures the source for records pulled
type Configuration struct{}

// DefaultConfiguration returns the default configuration for stdin source
func DefaultConfiguration() Configuration {
	return Configuration{}
}

// BuildFromConfig creates a stdin source from decoded configuration
func BuildFromConfig(cfg *Configuration) (sourceiface.Source, error) {
	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()

	return &stdinSourceDriver{
		log: log.WithFields(log.Fields{"source": SupportedSourceStdin}),
	}, nil
}

// stdinSourceDriver holds a new client for reading messages from stdin
type stdinSourceDriver struct {
	sourceiface.SourceChannels

	log *log.Entry
}

// NewStdinSourceDriver creates a new client for reading messages from stdin
func NewStdinSourceDriver() (sourceiface.Source, error) {

	// Ensures as even as possible distribution of UUIDs
	uuid.EnableRandPool()
	return &stdinSourceDriver{
		log: log.WithFields(log.Fields{"source": SupportedSourceStdin}),
	}, nil
}

// Read will execute until CTRL + D is pressed, until EOF is passed, or until context is cancelled
func (ss *stdinSourceDriver) Start(ctx context.Context) {
	defer close(ss.MessageChannel)
	ss.log.Infof("Reading messages from 'stdin', scanning until EOF detected or context cancelled (Note: Press 'CTRL + D' to exit)")

	lineChan := make(chan string)

	// Read from stdin in a goroutine
	go ss.consumeFromStdin(ctx, lineChan)

	// Process lines with context awareness
	for {
		select {
		case <-ctx.Done():
			ss.log.Info("Context cancelled, stopping stdin reader")
			return
		case line, ok := <-lineChan:
			if !ok {
				return
			}
			timeNow := time.Now().UTC()
			message := &models.Message{
				Data:         []byte(line),
				PartitionKey: uuid.New().String(),
				TimeCreated:  timeNow,
				TimePulled:   timeNow,
			}
			ss.MessageChannel <- message
		}
	}
}

func (ss *stdinSourceDriver) consumeFromStdin(ctx context.Context, lineChan chan string) {
	defer close(lineChan)
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		select {
		case lineChan <- scanner.Text():
		case <-ctx.Done():
			return
		}
	}
	if scanner.Err() != nil {
		ss.log.WithError(scanner.Err()).Error("Failed to read from stdin scanner")
	}
}
