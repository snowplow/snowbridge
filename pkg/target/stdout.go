// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// StdoutTarget holds a new client for writing messages to stdout
type StdoutTarget struct {
	log *log.Entry
}

// newStdoutTarget creates a new client for writing messages to stdout
func newStdoutTarget() (*StdoutTarget, error) {
	return &StdoutTarget{
		log: log.WithFields(log.Fields{"target": "stdout"}),
	}, nil
}

// StdoutTargetConfigFunction creates an StdoutTarget
func StdoutTargetConfigFunction() (*StdoutTarget, error) {
	return newStdoutTarget()
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
	return nil, nil
}

// AdaptStdoutTargetFunc returns StdoutTargetAdapter.
func AdaptStdoutTargetFunc(f func() (*StdoutTarget, error)) StdoutTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		if i != nil {
			return nil, errors.New("unexpected configuration input for Stdout target")
		}

		return f()
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
		fmt.Println(msg.String())

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
