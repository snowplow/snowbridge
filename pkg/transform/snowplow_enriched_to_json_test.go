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

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
)

func TestSpEnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	var messageGood = models.Message{
		Data:         SnowplowTsv1,
		PartitionKey: "some-key",
	}

	var messageBad = models.Message{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	}

	var expectedGood = models.Message{
		Data:         SnowplowJSON1,
		PartitionKey: "some-key",
	}

	// Simple success case
	transformSuccess, _, failure, intermediate := SpEnrichedToJSON(&messageGood, nil)

	assert.Equal(expectedGood.PartitionKey, transformSuccess.PartitionKey)
	assert.JSONEq(string(expectedGood.Data), string(transformSuccess.Data))
	assert.Equal(SpTsv1Parsed, intermediate)
	assert.Nil(failure)

	// Simple failure case
	success, _, transformFailure, intermediate := SpEnrichedToJSON(&messageBad, nil)

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.NotNil(transformFailure.GetError())
	if transformFailure.GetError() != nil {
		assert.Equal("intermediate state cannot be parsed as parsedEvent: cannot parse tsv event - wrong number of fields provided: 4", transformFailure.GetError().Error())
	}
	assert.Equal([]byte("not	a	snowplow	event"), transformFailure.Data)
	assert.Equal("some-key4", transformFailure.PartitionKey)
	// Failure in this case is in parsing to IntermediateState, so none expected in output
	assert.Nil(intermediate)
	assert.Nil(success)

	// Check that the input has not been altered
	assert.Nil(messageGood.GetError())

	// Nuanced success case
	// Test to assert behaviour when there's an incompatible IntermediateState in the input
	incompatibleIntermediateMessage := models.Message{
		Data:         SnowplowTsv1,
		PartitionKey: "some-key",
	}

	incompatibleIntermediate := "Incompatible intermediate state"

	// When we have some incompatible IntermediateState, expected behaviour is to replace it with this transformation's IntermediateState
	transformSuccess2, _, failure2, intermediate2 := SpEnrichedToJSON(&incompatibleIntermediateMessage, incompatibleIntermediate)

	assert.Equal(expectedGood.PartitionKey, transformSuccess2.PartitionKey)
	assert.JSONEq(string(expectedGood.Data), string(transformSuccess2.Data))
	assert.Equal(SpTsv1Parsed, intermediate2)
	assert.Nil(failure2)
}
