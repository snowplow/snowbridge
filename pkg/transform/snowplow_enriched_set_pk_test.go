// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/stretchr/testify/assert"
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

	stringAsPk, fail := aidSetPkFunc(&messageGood)

	assert.Equal("test-data", stringAsPk.PartitionKey)
	assert.Nil(fail)

	ctstampSetPkFunc := NewSpEnrichedSetPkFunction("collector_tstamp")

	tstampAsPk, fail := ctstampSetPkFunc(&messageGood)

	assert.Equal("2019-05-10 14:40:29.576 +0000 UTC", tstampAsPk.PartitionKey)
	assert.Nil(fail)

	pgurlportSetPkFunc := NewSpEnrichedSetPkFunction("page_urlport")

	intAsPk, failure := pgurlportSetPkFunc(&messageGood)

	assert.Equal("80", intAsPk.PartitionKey)
	assert.Nil(failure)

	// Simple failure case
	failureCase, fail := aidSetPkFunc(&messageBad)

	assert.Nil(failureCase)
	assert.NotNil(fail)
	assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", fail.GetError().Error())

	// Nuanced success case
	// Test to assert behaviour when there's an incompatible IntermediateState in the input
	incompatibleIntermediateMessage := models.Message{
		Data:              snowplowTsv1,
		PartitionKey:      "some-key",
		IntermediateState: "Incompatible intermediate state",
	}

	expected := models.Message{
		Data:              snowplowTsv1,
		PartitionKey:      "test-data",
		IntermediateState: spTsv1Parsed,
	}

	// When we have some incompatible IntermediateState, expected behaviour is to replace it with this transformation's IntermediateState
	stringAsPkIncompat, failIncompat := aidSetPkFunc(&incompatibleIntermediateMessage)

	assert.Equal(&expected, stringAsPkIncompat)
	assert.Nil(failIncompat)
}
