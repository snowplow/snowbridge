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
