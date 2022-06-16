// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

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
