// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestStdoutTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

	target, err := NewStdoutTarget()
	assert.NotNil(target)
	assert.Nil(err)

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
	assert.Equal(int64(1), writeRes.Sent)
	assert.Equal(int64(0), writeRes.Failed)
}
