// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

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
