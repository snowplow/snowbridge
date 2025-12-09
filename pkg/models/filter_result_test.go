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

package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewFilterResult_EmptyWithoutTime(t *testing.T) {
	assert := assert.New(t)

	r := NewFilterResult(nil)
	assert.NotNil(r)

	assert.Equal(int64(0), r.FilteredCount)

	assert.Equal(time.Duration(0), r.MaxFilterLatency)
	assert.Equal(time.Duration(0), r.MinFilterLatency)
}

func TestNewFilterResult_EmptyWithTime(t *testing.T) {
	assert := assert.New(t)

	r := newFilterResultWithTime(nil, time.Now().UTC())
	assert.NotNil(r)

	assert.Equal(int64(0), r.FilteredCount)

	assert.Equal(time.Duration(0), r.MaxFilterLatency)
	assert.Equal(time.Duration(0), r.MinFilterLatency)
}

func TestNewFilterResult_WithMessages(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now().UTC()

	filtered := []*Message{
		{
			Data:            []byte("Baz"),
			PartitionKey:    "partition1",
			TimeCreated:     timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-4) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-2) * time.Minute),
		},
		{
			Data:            []byte("Bar"),
			PartitionKey:    "partition2",
			TimeCreated:     timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:      timeNow.Add(time.Duration(-8) * time.Minute),
			TimeTransformed: timeNow.Add(time.Duration(-4) * time.Minute),
		},
	}

	r := newFilterResultWithTime(filtered, timeNow)
	assert.NotNil(r)

	assert.Equal(int64(2), r.FilteredCount)

	assert.Equal(time.Duration(8)*time.Minute, r.MaxFilterLatency)
	assert.Equal(time.Duration(4)*time.Minute, r.MinFilterLatency)
}
