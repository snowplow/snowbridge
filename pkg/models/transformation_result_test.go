//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewTransformationResult test NewTransformationResult.
// It doesn't do a whole lot so we don't need much here.
func TestNewTransformationResult(t *testing.T) {
	assert := assert.New(t)

	msgs := []*Message{
		{
			Data:         []byte("Baz"),
			PartitionKey: "partition1",
		},
		{
			Data:         []byte("Bar"),
			PartitionKey: "partition2",
		},
	}

	res := NewTransformationResult(msgs, msgs, msgs)

	assert.Equal(int64(2), res.ResultCount)
	assert.Equal(int64(2), res.FilteredCount)
	assert.Equal(int64(2), res.InvalidCount)
	assert.Equal(msgs, res.Result)
	assert.Equal(msgs, res.Filtered)
	assert.Equal(msgs, res.Invalid)
}
