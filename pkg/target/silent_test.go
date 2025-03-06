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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/testutil"
)

func TestSilentTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

  target := SilentTarget{}
	assert.Equal("silent", target.GetID())

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
