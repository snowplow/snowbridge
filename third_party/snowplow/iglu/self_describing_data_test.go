//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package iglu

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSelfDescribingData(t *testing.T) {
	assert := assert.New(t)

	sdd := NewSelfDescribingData(
		"iglu:com.acme/test/jsonschema/1-0-0",
		map[string]interface{}{
			"hello": "world",
			"foo":   10,
			"yes":   true,
		},
	)

	assert.NotNil(sdd)
	assert.Equal("iglu:com.acme/test/jsonschema/1-0-0", sdd.Get()["schema"])

	sddString, err := sdd.String()
	assert.Nil(err)
	assert.NotNil(sddString)
}

func TestNewSelfDescribingData_InvalidData(t *testing.T) {
	assert := assert.New(t)

	sdd := NewSelfDescribingData(
		"iglu:com.acme/test/jsonschema/1-0-0",
		map[bool]string{true: "pv"},
	)

	assert.NotNil(sdd)
	assert.Equal("iglu:com.acme/test/jsonschema/1-0-0", sdd.Get()["schema"])

	sddString, err := sdd.String()
	assert.NotNil(err)
	if err != nil {
		assert.Equal("json: unsupported type: map[bool]string", err.Error())
	}
	assert.Equal("", sddString)
}
