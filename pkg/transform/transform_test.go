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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// To test a function which creates a function, we're creating the function then testing that. Not sure if there's a better way?
func TestNewTransformation_Passthrough(t *testing.T) {
	assert := assert.New(t)

	noTransform := NewTransformation(make([]TransformationFunction, 0)...)

	// Test each message individually since we now process single messages
	for i, msg := range Messages {
		result := noTransform(msg)

		// With no transformations, each message should pass through as success
		assert.NotNil(result.Transformed)
		assert.Nil(result.Filtered)
		assert.Nil(result.Invalid)

		// Data should be unchanged
		assert.Equal(Messages[i].Data, result.Transformed.Data)
		assert.Equal(Messages[i].PartitionKey, result.Transformed.PartitionKey)
	}
}

func TestNewTransformation_EnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = [][]byte{
		SnowplowJSON1,
		snowplowJSON2,
		snowplowJSON3,
	}

	tranformEnrichJSON := NewTransformation(SpEnrichedToJSON)

	// Test the first three messages which should succeed
	for i := 0; i < 3; i++ {
		result := tranformEnrichJSON(Messages[i])

		assert.NotNil(result.Transformed)
		assert.Nil(result.Filtered)
		assert.Nil(result.Invalid)

		assert.JSONEq(string(expectedGood[i]), string(result.Transformed.Data))
		assert.Equal(Messages[i].PartitionKey, result.Transformed.PartitionKey)
		assert.NotNil(result.Transformed.TimeTransformed)

		// assertions to ensure we don't accidentally modify the input
		assert.NotEqual(Messages[i].Data, result.Transformed.Data)
		assert.Equal(time.Time{}, Messages[i].TimeTransformed)
	}

	// Test the fourth message which should fail
	result := tranformEnrichJSON(Messages[3])
	assert.Nil(result.Transformed)
	assert.Nil(result.Filtered)
	assert.NotNil(result.Invalid)
	assert.NotNil(result.Invalid.GetError())
	if result.Invalid.GetError() != nil {
		assert.Equal("intermediate state cannot be parsed as parsedEvent: cannot parse tsv event - wrong number of fields provided: 4", result.Invalid.GetError().Error())
	}
	assert.Equal([]byte("not	a	snowplow	event"), result.Invalid.Data)
	assert.Equal("some-key4", result.Invalid.PartitionKey)
}

func Benchmark_Transform_EnrichToJson(b *testing.B) {
	tranformEnrichJSON := NewTransformation(SpEnrichedToJSON)
	for b.Loop() {
		// Benchmark with a single message
		tranformEnrichJSON(Messages[0])
	}
}

func testfunc(message *models.Message, intermediateState any) (*models.Message, *models.Message, *models.Message, any) {
	return message, nil, nil, nil
}

func Benchmark_Transform_Passthrough(b *testing.B) {
	tranformPassthrough := NewTransformation(testfunc)
	for b.Loop() {
		// Benchmark with a single message
		tranformPassthrough(Messages[0])
	}
}

func TestNewTransformation_Multiple(t *testing.T) {
	assert := assert.New(t)

	var expectedGood = []struct {
		data         []byte
		partitionKey string
	}{
		{
			data:         SnowplowJSON1,
			partitionKey: "test-data1",
		},
		{
			data:         snowplowJSON2,
			partitionKey: "test-data2",
		},
		{
			data:         snowplowJSON3,
			partitionKey: "test-data3",
		},
	}

	setPkToAppID, _ := NewSpEnrichedSetPkFunction("app_id")
	tranformMultiple := NewTransformation(setPkToAppID, SpEnrichedToJSON)

	// Test the first three messages which should succeed
	for i := 0; i < 3; i++ {
		result := tranformMultiple(Messages[i])

		assert.NotNil(result.Transformed)
		assert.Nil(result.Filtered)
		assert.Nil(result.Invalid)

		assert.JSONEq(string(expectedGood[i].data), string(result.Transformed.Data))
		assert.Equal(expectedGood[i].partitionKey, result.Transformed.PartitionKey)
		assert.NotNil(result.Transformed.TimeTransformed)

		// assertions to ensure we don't accidentally modify the input
		assert.NotEqual(Messages[i].Data, result.Transformed.Data)
		assert.NotEqual(Messages[i].PartitionKey, result.Transformed.PartitionKey)
		// assert can't seem to deal with comparing zero value to non-zero value, so assert that it's still zero instead
		assert.Equal(time.Time{}, Messages[i].TimeTransformed)
	}

	// Test the fourth message which should fail
	result := tranformMultiple(Messages[3])
	assert.Nil(result.Transformed)
	assert.Nil(result.Filtered)
	assert.NotNil(result.Invalid)
	assert.NotNil(result.Invalid.GetError())
	if result.Invalid.GetError() != nil {
		assert.Equal("intermediate state cannot be parsed as parsedEvent: cannot parse tsv event - wrong number of fields provided: 4", result.Invalid.GetError().Error())
	}

	assert.Equal([]byte("not	a	snowplow	event"), result.Invalid.Data)
	assert.Equal("some-key4", result.Invalid.PartitionKey)
}
