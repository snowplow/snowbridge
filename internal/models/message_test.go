// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetChunkedMessages(t *testing.T) {
	assert := assert.New(t)

	messages := []*Message{
		{
			Data:         []byte("Hello World!"),
			PartitionKey: "some-key",
		},
		{
			Data:         []byte("Hello World1!"),
			PartitionKey: "some-key1",
		},
		{
			Data:         []byte("Hello World2!"),
			PartitionKey: "some-key2",
		},
		{
			Data:         []byte("Hello World3!"),
			PartitionKey: "some-key3",
		},
		{
			Data:         []byte("Hello World4!"),
			PartitionKey: "some-key4",
		},
	}

	res := GetChunkedMessages(messages, 2, 1000, 1000)
	assert.Equal(3, len(res))
	assert.Equal(2, len(res[0]))
	assert.Equal(2, len(res[1]))
	assert.Equal(1, len(res[2]))

	res1 := GetChunkedMessages(messages, 1000, 2, 1000)
	assert.Equal(5, len(res1))
	assert.Equal(1, len(res1[0]))
	assert.Equal(1, len(res1[1]))
	assert.Equal(1, len(res1[2]))
	assert.Equal(1, len(res1[3]))
	assert.Equal(1, len(res1[4]))

	res2 := GetChunkedMessages(messages, 1000, 1000, 2)
	assert.Equal(5, len(res2))
	assert.Equal(1, len(res2[0]))
	assert.Equal(1, len(res2[1]))
	assert.Equal(1, len(res2[2]))
	assert.Equal(1, len(res2[3]))
	assert.Equal(1, len(res2[4]))
}
