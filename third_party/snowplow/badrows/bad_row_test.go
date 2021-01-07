// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package badrows

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewBadRow_InvalidData(t *testing.T) {
	assert := assert.New(t)

	schema := "iglu:com.acme/event/jsonschema/1-0-0"

	data := map[string]interface{}{
		"hello": map[bool]string{
			true: "pv",
		},
	}

	br, err := NewBadRow(schema, data, []byte("Hello World!"), 5000)
	assert.NotNil(err)
	assert.Nil(br)
}