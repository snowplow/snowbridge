/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

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
