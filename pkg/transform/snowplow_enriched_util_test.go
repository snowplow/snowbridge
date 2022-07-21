// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/stretchr/testify/assert"
)

// TestIntermediateAsSpEnrichedParsed tests that intermediateAsSpEnrichedParsed
// returns the parsed event when provided a snowplow TSV with
func TestIntermediateAsSpEnrichedParsed(t *testing.T) {
	assert := assert.New(t)

	// case 1: no intermediate state
	res1, err1 := IntermediateAsSpEnrichedParsed(nil, &models.Message{Data: snowplowTsv1})

	assert.Equal(spTsv1Parsed, res1)
	assert.Nil(err1)

	// case 2: intermediate state provided as ParsedEvent
	res2, err2 := IntermediateAsSpEnrichedParsed(spTsv2Parsed, &models.Message{Data: snowplowTsv2})

	assert.Equal(spTsv2Parsed, res2)
	assert.Nil(err2)

	// case 3: intermediate state provided as some other type
	res3, err3 := IntermediateAsSpEnrichedParsed("not a ParsedEvent", &models.Message{Data: snowplowTsv3})

	assert.Equal(spTsv3Parsed, res3)
	assert.Nil(err3)

	// case 4: message not parseable
	res4, err4 := IntermediateAsSpEnrichedParsed(nil, &models.Message{Data: []byte("Not a snowplow event")})

	assert.Nil(res4)
	assert.NotNil(err4)
	if err4 != nil {
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 1", err4.Error())
	}
}

// TestConvertPathToInterfaces tests that convertPathToInterfaces returns integers and strings where appropriate
func TestConvertPathToInterfaces(t *testing.T) {
	assert := assert.New(t)

	expected := []interface{}{"one", 2, 3, "four", "five", 6}

	res := convertPathToInterfaces([]string{"one", "2", "3", "four", "five", "6"})

	assert.Equal(expected, res)
}
