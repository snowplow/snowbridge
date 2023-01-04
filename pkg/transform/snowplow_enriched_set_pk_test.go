//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
)

func TestNewSpEnrichedSetPkFunction(t *testing.T) {
	assert := assert.New(t)

	var messageGood = models.Message{
		Data:         SnowplowTsv3,
		PartitionKey: "some-key",
	}

	var messageBad = models.Message{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	}

	// Simple success cases for different datatypes
	aidSetPkFunc, _ := NewSpEnrichedSetPkFunction("app_id")

	stringAsPk, _, fail, intermediate := aidSetPkFunc(&messageGood, nil)

	assert.Equal("test-data3", stringAsPk.PartitionKey)
	assert.Equal(SpTsv3Parsed, intermediate)
	assert.Nil(fail)

	ctstampSetPkFunc, _ := NewSpEnrichedSetPkFunction("collector_tstamp")

	tstampAsPk, _, fail, intermediate := ctstampSetPkFunc(&messageGood, nil)

	assert.Equal("2019-05-10 14:40:29.576 +0000 UTC", tstampAsPk.PartitionKey)
	assert.Equal(SpTsv3Parsed, intermediate)
	assert.Nil(fail)

	pgurlportSetPkFunc, _ := NewSpEnrichedSetPkFunction("page_urlport")

	intAsPk, _, fail, intermediate := pgurlportSetPkFunc(&messageGood, nil)

	assert.Equal("80", intAsPk.PartitionKey)
	assert.Equal(SpTsv3Parsed, intermediate)
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
		Data:         SnowplowTsv1,
		PartitionKey: "some-key",
	}

	expected := models.Message{
		Data:         SnowplowTsv1,
		PartitionKey: "test-data1",
	}
	incompatibleIntermediate := "Incompatible intermediate state"

	// When we have some incompatible intermediateState, expected behaviour is to replace it with this transformation's intermediateState
	stringAsPkIncompat, _, failIncompat, intermediate := aidSetPkFunc(&incompatibleIntermediateMessage, incompatibleIntermediate)

	assert.Equal(&expected, stringAsPkIncompat)
	assert.Equal(SpTsv1Parsed, intermediate)
	assert.Nil(failIncompat)

	// Invalid field
	invalidFieldFunc, err := NewSpEnrichedSetPkFunction("notAnAtomicField")

	assert.Nil(invalidFieldFunc)
	assert.NotNil(err)
	fmt.Println(err)
}
