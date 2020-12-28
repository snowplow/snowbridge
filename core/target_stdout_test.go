// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStdoutTarget_WriteSuccess(t *testing.T) {
	assert := assert.New(t)

	target, err := NewStdoutTarget()
	assert.NotNil(target)
	assert.Nil(err)

	dataStr := "Hello World!"
	var ackStr string

	ackFunc := func() {
		ackStr = "Hello World!"
	}

	assert.NotEqual(dataStr, ackStr)

	messages := []*Message{
		{
			Data:         []byte("Hello World!"),
			PartitionKey: "some-key",
			AckFunc:      ackFunc,
		},
	}

	_, err1 := target.Write(messages)
	assert.Nil(err1)
	target.Close()

	assert.Equal(dataStr, ackStr)
}
