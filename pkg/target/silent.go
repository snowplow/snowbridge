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
	"github.com/snowplow/snowbridge/pkg/models"
)

// SilentTarget holds a new client for silently acking data
type SilentTarget struct{}

// SilentTargetConfigFunction creates an SilentTarget
func SilentTargetConfigFunction() (*SilentTarget, error) {
	return &SilentTarget{}, nil
}

// The SilentTargetAdapter type is an adapter for functions to be used as
// pluggable components for Silent Target. It implements the Pluggable interface.
type SilentTargetAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f SilentTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f SilentTargetAdapter) ProvideDefault() (interface{}, error) {
	return nil, nil
}

// AdaptSilentTargetFunc returns SilentTargetAdapter.
func AdaptSilentTargetFunc(f func() (*SilentTarget, error)) SilentTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		if i != nil {
			return nil, errors.New("unexpected configuration input for Silent target")
		}

		return f()
	}
}

// Write pushes all messages to the required target
// It's just acking data, nothing more
func (st *SilentTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	for _, msg := range messages {
		if msg.AckFunc != nil {
			msg.AckFunc()
		}
	}

	return models.NewTargetWriteResult(
		messages,
		nil,
		nil,
		nil,
	), nil
}

// Open does not do anything for this target
func (st *SilentTarget) Open() {}

// Close does not do anything for this target
func (st *SilentTarget) Close() {}

// MaximumAllowedMessageSizeBytes returns the max number of bytes that can be sent
// per message for this target
//
// Note: Technically no limit but we are putting in a limit of 10 MiB here
// to avoid trying to print out huge payloads
func (st *SilentTarget) MaximumAllowedMessageSizeBytes() int {
	return 10485760
}

// GetID returns the identifier for this target
func (st *SilentTarget) GetID() string {
	return "silent"
}
