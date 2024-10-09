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

package transform

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
)

// To test a function which creates a function, we're creating the function then testing that. Not sure if there's a better way?
func TestNewTransformation_Passthrough(t *testing.T) {
	assert := assert.New(t)

	// expected is equal to messages, specifying separately to avoid false positive if we accidentally mutate input.
	var expected = []*models.Message{
		{
			Data:         SnowplowTsv1,
			PartitionKey: "some-key",
		},
		{
			Data:         SnowplowTsv2,
			PartitionKey: "some-key1",
		},
		{
			Data:         SnowplowTsv3,
			PartitionKey: "some-key2",
		},
		{
			Data:         []byte(`not	a	snowplow	event`),
			PartitionKey: "some-key4",
		},
	}

	expectedNoTransformRes := models.NewTransformationResult(expected, make([]*models.Message, 0, 0), make([]*models.Message, 0, 0))
	noTransform := NewTransformation(make([]TransformationFunction, 0, 0)...)
	noTransformResult := noTransform(Messages)

	assert.Equal(expectedNoTransformRes, noTransformResult)
}

func TestNewTransformation_EnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []*models.Message{
		{
			Data:         SnowplowJSON1,
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
	enrichJSONRes := tranformEnrichJSON(Messages)

	for index, value := range enrichJSONRes.Result {
		assert.JSONEq(string(expectedGood[index].Data), string(value.Data))
		assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
		assert.NotNil(expectedGood[index].TimeTransformed)

		// assertions to ensure we don't accidentally modify the input
		assert.NotEqual(Messages[index].Data, value.Data)
		// assert can't seem to deal with comparing zero value to non-zero value, so assert that it's still zero instead
		assert.Equal(time.Time{}, Messages[index].TimeTransformed)
	}

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.Equal(1, len(enrichJSONRes.Invalid))
	assert.Equal(int64(1), enrichJSONRes.InvalidCount)
	assert.NotNil(enrichJSONRes.Invalid[0].GetError())
	if enrichJSONRes.Invalid[0].GetError() != nil {
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJSONRes.Invalid[0].GetError().Error())
	}
	assert.Equal([]byte("not	a	snowplow	event"), enrichJSONRes.Invalid[0].Data)
	assert.Equal("some-key4", enrichJSONRes.Invalid[0].PartitionKey)
}

func Benchmark_Transform_EnrichToJson(b *testing.B) {
	tranformEnrichJSON := NewTransformation(SpEnrichedToJSON)
	for i := 0; i < b.N; i++ {
		tranformEnrichJSON(Messages)
	}
}

func testfunc(message *models.Message, intermediateState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
	return message, nil, nil, nil
}

func Benchmark_Transform_Passthrough(b *testing.B) {
	tranformPassthrough := NewTransformation(testfunc)
	for i := 0; i < b.N; i++ {
		tranformPassthrough(Messages)
	}
}

func TestNewTransformation_Multiple(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []*models.Message{
		{
			Data:         SnowplowJSON1,
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

	setPkToAppID, _ := NewSpEnrichedSetPkFunction("app_id")
	tranformMultiple := NewTransformation(setPkToAppID, SpEnrichedToJSON)

	enrichJSONRes := tranformMultiple(Messages)

	for index, value := range enrichJSONRes.Result {
		assert.JSONEq(string(expectedGood[index].Data), string(value.Data))
		assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
		assert.NotNil(expectedGood[index].TimeTransformed)
		assert.NotNil(value.TimeTransformed)

		// assertions to ensure we don't accidentally modify the input
		assert.NotEqual(Messages[index].Data, value.Data)
		assert.NotEqual(Messages[index].PartitionKey, value.PartitionKey)
		// assert can't seem to deal with comparing zero value to non-zero value, so assert that it's still zero instead
		assert.Equal(time.Time{}, Messages[index].TimeTransformed)
	}

	// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
	assert.Equal(1, len(enrichJSONRes.Invalid))
	assert.Equal(int64(1), enrichJSONRes.InvalidCount)
	assert.NotNil(enrichJSONRes.Invalid[0].GetError())
	if enrichJSONRes.Invalid[0].GetError() != nil {
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJSONRes.Invalid[0].GetError().Error())
	}

	assert.Equal([]byte("not	a	snowplow	event"), enrichJSONRes.Invalid[0].Data)
	assert.Equal("some-key4", enrichJSONRes.Invalid[0].PartitionKey)
}
