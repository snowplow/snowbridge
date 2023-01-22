//
// Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package collectorpayload

import (
	"context"
	"testing"

	model1 "github.com/snowplow/snowbridge/third_party/snowplow/collectorpayload/gen-go/model1"

	"github.com/stretchr/testify/assert"
)

// TestBinarySerializer
func TestBinarySerializerAndDeserializer(t *testing.T) {
	ctx := context.Background()

	assert := assert.New(t)

	payload := model1.NewCollectorPayload()
	payload.IpAddress = "192.168.0.1"

	res, err := BinarySerializer(ctx, payload)
	assert.Nil(err)
	assert.NotNil(res)

	res1, err1 := BinaryDeserializer(ctx, res)
	assert.Nil(err1)
	assert.NotNil(res1)
	assert.Equal("192.168.0.1", res1.IpAddress)
}
