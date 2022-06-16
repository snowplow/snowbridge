// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package badrows

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewBadRow_InvalidData(t *testing.T) {
	assert := assert.New(t)

	schema := "iglu:com.acme/event/jsonschema/1-0-0"

	data := map[string]interface{}{
		"hello": map[bool]string{
			true: "pv",
		},
	}

	br, err := newBadRow(schema, data, []byte("Hello World!"), 5000)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Could not unmarshall bad-row data blob to JSON: json: unsupported type: map[bool]string", err.Error())
	}
	assert.Nil(br)
}
