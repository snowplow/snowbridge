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

	expectedNoTransformRes := models.NewTransformationResult(expected, make([]*models.Message, 0, 0), make([]*models.Message, 0, 0))
	noTransform := NewTransformation(make([]TransformationFunction, 0, 0)...)
	noTransformResult := noTransform(messages)

	assert.Equal(expectedNoTransformRes, noTransformResult)
}

func TestNewTransformation_EnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []*models.Message{
		{
			Data:         snowplowJSON1,
			PartitionKey: "some-key",
		},
		{
			Data:         snowplowJSON2,
			PartitionKey: "some-key1",
		},
		{
			Data:         snowplowJSON3,
			PartitionKey: "some-key2",
		},
	}

	tranformEnrichJSON := NewTransformation(SpEnrichedToJSON)
	enrichJSONRes := tranformEnrichJSON(messages)

	for index, value := range enrichJSONRes.Result {
		assert.Equal(expectedGood[index].Data, value.Data)
		assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
		assert.NotNil(expectedGood[index].TimeTransformed)

		// assertions to ensure we don't accidentally modify the input
		assert.NotEqual(messages[index].Data, value.Data)
		// assert can't seem to deal with comparing zero value to non-zero value, so assert that it's still zero instead
		assert.Equal(time.Time{}, messages[index].TimeTransformed)
	}

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.Equal(1, len(enrichJSONRes.Invalid))
	assert.Equal(int64(1), enrichJSONRes.InvalidCount)
	assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJSONRes.Invalid[0].GetError().Error())
	assert.Equal([]byte("not	a	snowplow	event"), enrichJSONRes.Invalid[0].Data)
	assert.Equal("some-key4", enrichJSONRes.Invalid[0].PartitionKey)
}

func Benchmark_Transform_EnrichToJson(b *testing.B) {
	tranformEnrichJSON := NewTransformation(SpEnrichedToJSON)
	for i := 0; i < b.N; i++ {
		tranformEnrichJSON(messages)
	}
}

func testfunc(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
	return message, nil, nil, nil
}

func Benchmark_Transform_Passthrough(b *testing.B) {
	tranformPassthrough := NewTransformation(testfunc)
	for i := 0; i < b.N; i++ {
		tranformPassthrough(messages)
	}
}

func TestNewTransformation_Multiple(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []*models.Message{
		{
			Data:         snowplowJSON1,
			PartitionKey: "test-data1",
		},
		{
			Data:         snowplowJSON2,
			PartitionKey: "test-data2",
		},
		{
			Data:         snowplowJSON3,
			PartitionKey: "test-data3",
		},
	}

	setPkToAppID := NewSpEnrichedSetPkFunction("app_id")
	tranformMultiple := NewTransformation(setPkToAppID, SpEnrichedToJSON)

	enrichJSONRes := tranformMultiple(messages)

	for index, value := range enrichJSONRes.Result {
		assert.Equal(expectedGood[index].Data, value.Data)
		assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
		assert.NotNil(expectedGood[index].TimeTransformed)

		// assertions to ensure we don't accidentally modify the input
		assert.NotEqual(messages[index].Data, value.Data)
		assert.NotEqual(messages[index].PartitionKey, value.PartitionKey)
		// assert can't seem to deal with comparing zero value to non-zero value, so assert that it's still zero instead
		assert.Equal(time.Time{}, messages[index].TimeTransformed)
	}

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.Equal(1, len(enrichJSONRes.Invalid))
	assert.Equal(int64(1), enrichJSONRes.InvalidCount)
	assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJSONRes.Invalid[0].GetError().Error())
	assert.Equal([]byte("not	a	snowplow	event"), enrichJSONRes.Invalid[0].Data)
	assert.Equal("some-key4", enrichJSONRes.Invalid[0].PartitionKey)
}
