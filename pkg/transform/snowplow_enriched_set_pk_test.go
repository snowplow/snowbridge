// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func TestNewSpEnrichedSetPkFunction(t *testing.T) {
	assert := assert.New(t)

	var messageGood = models.Message{
		Data:         snowplowTsv3,
		PartitionKey: "some-key",
	}

	var messageBad = models.Message{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	}

	// Simple success cases for different datatypes
	aidSetPkFunc := NewSpEnrichedSetPkFunction("app_id")

	stringAsPk, _, fail, intermediate := aidSetPkFunc(&messageGood, nil)

	assert.Equal("test-data3", stringAsPk.PartitionKey)
	assert.Equal(spTsv3Parsed, intermediate)
	assert.Nil(fail)

	ctstampSetPkFunc := NewSpEnrichedSetPkFunction("collector_tstamp")

	tstampAsPk, _, fail, intermediate := ctstampSetPkFunc(&messageGood, nil)

	assert.Equal("2019-05-10 14:40:29.576 +0000 UTC", tstampAsPk.PartitionKey)
	assert.Equal(spTsv3Parsed, intermediate)
	assert.Nil(fail)

	pgurlportSetPkFunc := NewSpEnrichedSetPkFunction("page_urlport")

	intAsPk, _, fail, intermediate := pgurlportSetPkFunc(&messageGood, nil)

	assert.Equal("80", intAsPk.PartitionKey)
	assert.Equal(spTsv3Parsed, intermediate)
	assert.Nil(fail)

	// Simple failure case
	failureCase, _, fail, intermediate := aidSetPkFunc(&messageBad, nil)

	assert.Nil(failureCase)
	assert.Nil(intermediate)
	assert.NotNil(fail)
	assert.NotNil(fail.GetError())
	if fail.GetError() != nil {
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", fail.GetError().Error())
	}

	// Nuanced success case
	// Test to assert behaviour when there's an incompatible intermediateState in the input
	incompatibleIntermediateMessage := models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}

	expected := models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "test-data1",
	}
	incompatibleIntermediate := "Incompatible intermediate state"

	// When we have some incompatible intermediateState, expected behaviour is to replace it with this transformation's intermediateState
	stringAsPkIncompat, _, failIncompat, intermediate := aidSetPkFunc(&incompatibleIntermediateMessage, incompatibleIntermediate)

	assert.Equal(&expected, stringAsPkIncompat)
	assert.Equal(spTsv1Parsed, intermediate)
	assert.Nil(failIncompat)
}
