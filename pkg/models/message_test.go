//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package models

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestMessageString(t *testing.T) {
	assert := assert.New(t)

	msg := Message{
		Data:         []byte("Hello World!"),
		PartitionKey: "some-key",
	}

	assert.Equal("PartitionKey:some-key,TimeCreated:0001-01-01 00:00:00 +0000 UTC,TimePulled:0001-01-01 00:00:00 +0000 UTC,TimeTransformed:0001-01-01 00:00:00 +0000 UTC,Data:Hello World!", msg.String())
	assert.Nil(msg.GetError())

	msg.SetError(errors.New("failure"))

	assert.NotNil(msg.GetError())
	if msg.GetError() != nil {
		assert.Equal("failure", msg.GetError().Error())
	}
}

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

	res, oversized := GetChunkedMessages(messages, 2, 1000, 1000)
	assert.Equal(3, len(res))
	assert.Equal(0, len(oversized))
	assert.Equal(2, len(res[0]))
	assert.Equal(2, len(res[1]))
	assert.Equal(1, len(res[2]))

	res1, oversized1 := GetChunkedMessages(messages, 1000, 2, 1000)
	assert.Equal(0, len(res1))
	assert.Equal(5, len(oversized1))

	res2, oversized2 := GetChunkedMessages(messages, 1000, 1000, 2)
	assert.Equal(5, len(res2))
	assert.Equal(0, len(oversized2))
	assert.Equal(1, len(res2[0]))
	assert.Equal(1, len(res2[1]))
	assert.Equal(1, len(res2[2]))
	assert.Equal(1, len(res2[3]))
	assert.Equal(1, len(res2[4]))
}

func TestFilterOversizedMessages(t *testing.T) {
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
			Data:         []byte("Hello World4! This will be too long!"),
			PartitionKey: "some-key4",
		},
	}

	safe, oversized := FilterOversizedMessages(messages, 20)
	assert.Equal(4, len(safe))
	assert.Equal(1, len(oversized))
}
