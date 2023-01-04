//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package target

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestStdoutTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

	target, err := newStdoutTarget()
	assert.NotNil(target)
	assert.Nil(err)
	assert.Equal("stdout", target.GetID())

	defer target.Close()
	target.Open()

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(1, "Hello World!", ackFunc)

	writeRes, err := target.Write(messages)
	assert.Nil(err)
	assert.NotNil(writeRes)

	// Check that Ack is called
	assert.Equal(int64(1), ackOps)

	// Check results
	assert.Equal(int64(1), writeRes.SentCount)
	assert.Equal(int64(0), writeRes.FailedCount)
	assert.Equal(0, len(writeRes.Oversized))
}
