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

package stdout

import (
	"bytes"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestStdoutTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

	target := StdoutTargetDriver{}
	err := target.initWithInterfaces(os.Stdout, false)
	assert.NotNil(target)
	assert.Nil(err)

	defer target.Close()
	err = target.Open()
	assert.Nil(err)

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
	assert.Equal(1, len(writeRes.Sent))
	assert.Equal(0, len(writeRes.Failed))
}

func TestStdoutTarget_WriteFullMessage(t *testing.T) {
	var output bytes.Buffer
	assert := assert.New(t)

	wantOutput := "TimeCreated:0001-01-01 00:00:00 +0000 UTC,TimePulled:0001-01-01 00:00:00 +0000 UTC,TimeTransformed:0001-01-01 00:00:00 +0000 UTC,Data:Hello World!\n"

	target := StdoutTargetDriver{}
	err := target.initWithInterfaces(&output, false)
	assert.NotNil(target)
	assert.Nil(err)

	defer target.Close()
	err = target.Open()
	assert.Nil(err)

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(1, "Hello World!", ackFunc)

	writeRes, err := target.Write(messages)

	// First element is ever-changing PartitionKey, so remove it from comparison
	gotSlice := strings.Split(output.String(), ",")
	assert.Equal(wantOutput, strings.Join(gotSlice[1:], ","))

	assert.Nil(err)
	assert.NotNil(writeRes)
}

func TestStdoutTarget_WriteDataOnlyMessage(t *testing.T) {
	var output bytes.Buffer
	assert := assert.New(t)

	wantOutput := "Hello World!\n"

	target := StdoutTargetDriver{}
	err := target.initWithInterfaces(&output, true)
	assert.NotNil(target)
	assert.Nil(err)

	defer target.Close()
	err = target.Open()
	assert.Nil(err)

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(1, "Hello World!", ackFunc)

	writeRes, err := target.Write(messages)

	assert.Equal(wantOutput, output.String())
	assert.Nil(err)
	assert.NotNil(writeRes)
}
