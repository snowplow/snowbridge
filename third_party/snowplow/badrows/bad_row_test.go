//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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
