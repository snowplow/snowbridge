/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package transform

import (
	"testing"

	"github.com/snowplow/snowbridge/pkg/models"

	"github.com/stretchr/testify/assert"
)

// TestIntermediateAsSpEnrichedParsed tests that intermediateAsSpEnrichedParsed
// returns the parsed event when provided a snowplow TSV with
func TestIntermediateAsSpEnrichedParsed(t *testing.T) {
	assert := assert.New(t)

	// case 1: no intermediate state
	res1, err1 := IntermediateAsSpEnrichedParsed(nil, &models.Message{Data: SnowplowTsv1})

	assert.Equal(SpTsv1Parsed, res1)
	assert.Nil(err1)

	// case 2: intermediate state provided as ParsedEvent
	res2, err2 := IntermediateAsSpEnrichedParsed(SpTsv2Parsed, &models.Message{Data: SnowplowTsv2})

	assert.Equal(SpTsv2Parsed, res2)
	assert.Nil(err2)

	// case 3: intermediate state provided as some other type
	res3, err3 := IntermediateAsSpEnrichedParsed("not a ParsedEvent", &models.Message{Data: SnowplowTsv3})

	assert.Equal(SpTsv3Parsed, res3)
	assert.Nil(err3)

	// case 4: message not parseable
	res4, err4 := IntermediateAsSpEnrichedParsed(nil, &models.Message{Data: []byte("Not a snowplow event")})

	assert.Nil(res4)
	assert.NotNil(err4)
	if err4 != nil {
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 1", err4.Error())
	}
}
