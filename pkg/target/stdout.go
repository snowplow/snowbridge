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

package target

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/models"
)

// StdoutTargetConfig configures the destination for records consumed
type StdoutTargetConfig struct {
	DataOnlyOutput bool `hcl:"data_only_output,optional"`
}

// StdoutTarget holds a new client for writing messages to stdout
type StdoutTarget struct {
	output         io.Writer
	dataOnlyOutput bool

	log *log.Entry
}

// newStdoutTarget creates a new client for writing messages to stdout
func newStdoutTarget(dataOnlyOutput bool) (*StdoutTarget, error) {
	return newStdoutTargetWithInterfaces(os.Stdout, dataOnlyOutput)
}

// newStdoutTargettWithInterfaces allows you to provide an Stdout directly to allow
// for mocking and localstack usage
func newStdoutTargetWithInterfaces(writer io.Writer, dataOnlyOutput bool) (*StdoutTarget, error) {
	return &StdoutTarget{
		output:         writer,
		dataOnlyOutput: dataOnlyOutput,
		log:            log.WithFields(log.Fields{"target": "stdout"}),
	}, nil
}

// StdoutTargetConfigFunction creates an StdoutTarget
func StdoutTargetConfigFunction(c *StdoutTargetConfig) (*StdoutTarget, error) {
	return newStdoutTarget(c.DataOnlyOutput)
}

// The StdoutTargetAdapter type is an adapter for functions to be used as
// pluggable components for Stdout Target. It implements the Pluggable interface.
type StdoutTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f StdoutTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f StdoutTargetAdapter) ProvideDefault() (interface{}, error) {
	cfg := &StdoutTargetConfig{}

	return cfg, nil
}

// AdaptStdoutTargetFunc returns StdoutTargetAdapter.
func AdaptStdoutTargetFunc(f func(c *StdoutTargetConfig) (*StdoutTarget, error)) StdoutTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*StdoutTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StdoutTargetConfig")
		}

		return f(cfg)
	}
}

// Write pushes all messages to the required target
func (st *StdoutTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	st.log.Debugf("Writing %d messages to stdout ...", len(messages))

	safeMessages, oversized := models.FilterOversizedMessages(
		messages,
		st.MaximumAllowedMessageSizeBytes(),
	)

	var sent []*models.Message

	for _, msg := range safeMessages {
		msg.TimeRequestStarted = time.Now().UTC()
		if st.dataOnlyOutput {
			fmt.Fprintf(st.output, "%s\n", string(msg.Data))
		} else {
			fmt.Fprintf(st.output, "%s\n", msg.String())
		}
		msg.TimeRequestFinished = time.Now().UTC()

		if msg.AckFunc != nil {
			msg.AckFunc()
		}

		sent = append(sent, msg)
	}

	return models.NewTargetWriteResult(
		sent,
		nil,
		oversized,
		nil,
	), nil
}

// Open does not do anything for this target
func (st *StdoutTarget) Open() {}

// Close does not do anything for this target
func (st *StdoutTarget) Close() {}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
//
// Note: Technically no limit but we are putting in a limit of 10 MiB here
// to avoid trying to print out huge payloads
func (st *StdoutTarget) MaximumAllowedMessageSizeBytes() int {
	return 10485760
}

// GetID returns the identifier for this target
func (st *StdoutTarget) GetID() string {
	return "stdout"
}
