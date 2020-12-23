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

	ackFunc := func() {
		assert.Equal(dataStr, "Hello World!")
	}

	events := []*Event{
		{
			Data:         []byte("Hello World!"),
			PartitionKey: "some-key",
			AckFunc:      ackFunc,
		},
	}

	err1 := target.Write(events)
	assert.Nil(err1)
	target.Close()
}
