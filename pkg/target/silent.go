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

type SilentTarget struct {}

func SilentTargetConfigFunction() (*SilentTarget, error) {
	return &SilentTarget{}, nil 
}

type SilentTargetAdapter func(i interface{}) (interface{}, error)

func (f SilentTargetAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

func (f SilentTargetAdapter) ProvideDefault() (interface{}, error) {
	return nil, nil
}

func AdaptSilentTargetFunc(f func() (*SilentTarget, error)) SilentTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		if i != nil {
			return nil, errors.New("unexpected configuration input for Silent target")
		}

		return f()
	}
}

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

func (st *SilentTarget) Open() {}

func (st *SilentTarget) Close() {}

func (st *SilentTarget) GetID() string {
  return "silent"
}
