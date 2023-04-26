//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"testing"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestBase64DecodeSuccess(t *testing.T) {
	assert := assert.New(t)

	testCase := models.Message{
		Data: []byte("SGVsbG8gV29ybGQh"),
	}

	success, _, failure, _ := Base64Decode(&testCase, nil)

	assert.Equal("Hello World!", string(success.Data))
	assert.Nil(failure)

	assert.Nil(nil)
}

func TestBase64DecodeFailure(t *testing.T) {
	assert := assert.New(t)

	testCase := models.Message{
		Data: []byte("notB64"),
	}

	success, _, failure, _ := Base64Decode(&testCase, nil)

	assert.Nil(success)
	assert.NotNil(failure)
	assert.NotNil(failure.GetError())

	assert.Nil(nil)
}

func TestBase64EncodeSuccess(t *testing.T) {
	assert := assert.New(t)

	testCase := models.Message{
		Data: []byte("Hello World!"),
	}

	success, _, failure, _ := Base64Encode(&testCase, nil)

	assert.Equal("SGVsbG8gV29ybGQh", string(success.Data))
	assert.Nil(failure)

	assert.Nil(nil)
}
