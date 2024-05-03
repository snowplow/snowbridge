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
