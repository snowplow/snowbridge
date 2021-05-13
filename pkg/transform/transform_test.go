// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"
	"time"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/stretchr/testify/assert"
)

var messages = []*models.Message{
	{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	},
	{
		Data:         snowplowTsv2,
		PartitionKey: "some-key1",
	},
	{
		Data:         snowplowTsv3,
		PartitionKey: "some-key2",
	},
	{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	},
}

// To test a function which creates a function, we're creating the function then testing that. Not sure if there's a better way?
func TestNewTransformation_Passthrough(t *testing.T) {
	assert := assert.New(t)

	// expected is equal to messages, specifying separately to avoid false positive if we accidentally mutate input.
	var expected = []*models.Message{
		{
			Data:         snowplowTsv1,
			PartitionKey: "some-key",
		},
		{
			Data:         snowplowTsv2,
			PartitionKey: "some-key1",
		},
		{
			Data:         snowplowTsv3,
			PartitionKey: "some-key2",
		},
		{
			Data: []byte(`not	a	snowplow	event`),
			PartitionKey: "some-key4",
		},
	}

	expectedNoTransformRes := models.NewTransformationResult(expected, make([]*models.Message, 0, 0))
	noTransform := NewTransformation(make([]TransformationFunction, 0, 0)...)
	noTransformResult := noTransform(messages)

	assert.Equal(expectedNoTransformRes, noTransformResult)
}

func TestNewTransformation_EnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []*models.Message{
		{
			Data:              snowplowJson1,
			PartitionKey:      "some-key",
			IntermediateState: spTsv1Parsed,
		},
		{
			Data:              snowplowJson2,
			PartitionKey:      "some-key1",
			IntermediateState: spTsv2Parsed,
		},
		{
			Data:              snowplowJson3,
			PartitionKey:      "some-key2",
			IntermediateState: spTsv3Parsed,
		},
	}

	tranformEnrichJson := NewTransformation(SpEnrichedToJson)
	enrichJsonRes := tranformEnrichJson(messages)

	for index, value := range enrichJsonRes.Result {
		assert.Equal(expectedGood[index].Data, value.Data)
		assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
		assert.Equal(expectedGood[index].IntermediateState, value.IntermediateState)
		assert.NotNil(expectedGood[index].TimeTransformed)
		// assertion to ensure we don't accidentally modify the input
		assert.NotEqual(messages[index].Data, value.Data)
		// assert.NotEqual(messages[index].IntermediateState, value.IntermediateState)
		// The above fails... But should it???
		assert.Equal(time.Time{}, messages[index].TimeTransformed)
		// assert can't seem to deal with comparing the actual zero value, so assert that it's still zero instead
	}

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.Equal(1, len(enrichJsonRes.Invalid))
	assert.Equal(int64(1), enrichJsonRes.InvalidCount)
	assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJsonRes.Invalid[0].GetError().Error())
	assert.Equal([]byte("not	a	snowplow	event"), enrichJsonRes.Invalid[0].Data)
	assert.Equal("some-key4", enrichJsonRes.Invalid[0].PartitionKey)
}

func TestNewTransformation_Multiple(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []*models.Message{
		{
			Data:              snowplowJson1,
			PartitionKey:      "test-data",
			IntermediateState: spTsv1Parsed,
		},
		{
			Data:              snowplowJson2,
			PartitionKey:      "test-data",
			IntermediateState: spTsv2Parsed,
		},
		{
			Data:              snowplowJson3,
			PartitionKey:      "test-data",
			IntermediateState: spTsv3Parsed,
		},
	}

	setPkToAppId := NewSpEnrichedSetPkFunction("app_id")
	tranformMultiple := NewTransformation(setPkToAppId, SpEnrichedToJson)

	enrichJsonRes := tranformMultiple(messages)

	for index, value := range enrichJsonRes.Result {
		assert.Equal(expectedGood[index].Data, value.Data)
		assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
		assert.Equal(expectedGood[index].IntermediateState, value.IntermediateState)
		assert.NotNil(expectedGood[index].TimeTransformed)
		// assertion to ensure we don't accidentally modify the input
		assert.NotEqual(messages[index].Data, value.Data)
		assert.NotEqual(messages[index].PartitionKey, value.PartitionKey)
		// assert.NotEqual(messages[index].IntermediateState, value.IntermediateState)
		// The above fails... But should it???
		assert.Equal(time.Time{}, messages[index].TimeTransformed)
		// assert can't seem to deal with comparing the actual zero value, so assert that it's still zero instead
	}

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.Equal(1, len(enrichJsonRes.Invalid))
	assert.Equal(int64(1), enrichJsonRes.InvalidCount)
	assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJsonRes.Invalid[0].GetError().Error())
	assert.Equal([]byte("not	a	snowplow	event"), enrichJsonRes.Invalid[0].Data)
	assert.Equal("some-key4", enrichJsonRes.Invalid[0].PartitionKey)
}
