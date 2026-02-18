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

	"github.com/stretchr/testify/assert"
)

// TestNewTransformationResult test NewTransformationResult.
// It doesn't do a whole lot so we don't need much here.
func TestNewTransformationResult(t *testing.T) {
	assert := assert.New(t)

	successMsg := &Message{
		Data:         []byte("Success"),
		PartitionKey: "partition1",
	}
	filteredMsg := &Message{
		Data:         []byte("Filtered"),
		PartitionKey: "partition2",
	}
	invalidMsg := &Message{
		Data:         []byte("Invalid"),
		PartitionKey: "partition3",
	}

	res := NewTransformationResult(successMsg, filteredMsg, invalidMsg)

	assert.Equal(successMsg, res.Transformed)
	assert.Equal(filteredMsg, res.Filtered)
	assert.Equal(invalidMsg, res.Invalid)
}
