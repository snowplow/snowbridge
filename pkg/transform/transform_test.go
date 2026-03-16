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

package transform_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/transform"
	sp_enriched "github.com/snowplow/snowbridge/v3/pkg/transform/sp_enriched"
)

func TestNewTransformation_Passthrough(t *testing.T) {
	assert := assert.New(t)

	noTransform := transform.NewTransformation(make([]transform.TransformationFunction, 0)...)

	messages := []*models.Message{
		{Data: []byte("message-1"), PartitionKey: "key-1"},
		{Data: []byte("message-2"), PartitionKey: "key-2"},
	}

	for _, msg := range messages {
		result := noTransform(msg)

		assert.NotNil(result.Transformed)
		assert.Nil(result.Filtered)
		assert.Nil(result.Invalid)

		assert.Equal(msg.Data, result.Transformed.Data)
		assert.Equal(msg.PartitionKey, result.Transformed.PartitionKey)
		assert.Equal(msg.Data, result.Transformed.OriginalData)
	}
}

func TestNewTransformation_EnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		input    []byte
		key      string
		expected []byte
	}{
		{input: transform.SnowplowTsv1, key: "key-1", expected: transform.SnowplowJSON1},
		{input: transform.SnowplowTsv2, key: "key-2", expected: transform.SnowplowJSON2},
		{input: transform.SnowplowTsv3, key: "key-3", expected: transform.SnowplowJSON3},
	}

	transformEnrichJSON := transform.NewTransformation(sp_enriched.SpEnrichedToJSON)

	for _, tc := range testCases {
		msg := &models.Message{Data: tc.input, PartitionKey: tc.key}
		result := transformEnrichJSON(msg)

		assert.NotNil(result.Transformed)
		assert.Nil(result.Filtered)
		assert.Nil(result.Invalid)

		assert.JSONEq(string(tc.expected), string(result.Transformed.Data))
		assert.Equal(tc.key, result.Transformed.PartitionKey)
		assert.NotNil(result.Transformed.TimeTransformed)
		assert.Equal(tc.input, result.Transformed.OriginalData)
	}

	// Test with invalid Snowplow event
	msg := &models.Message{Data: []byte("not\ta\tsnowplow\tevent"), PartitionKey: "some-key4"}
	result := transformEnrichJSON(msg)

	assert.Nil(result.Transformed)
	assert.Nil(result.Filtered)
	assert.NotNil(result.Invalid)
	assert.NotNil(result.Invalid.GetError())
	if result.Invalid.GetError() != nil {
		assert.Equal("intermediate state cannot be parsed as parsedEvent: cannot parse tsv event - wrong number of fields provided: 4", result.Invalid.GetError().Error())
	}
	assert.Equal([]byte("not\ta\tsnowplow\tevent"), result.Invalid.Data)
	assert.Equal("some-key4", result.Invalid.PartitionKey)
	assert.Equal([]byte("not\ta\tsnowplow\tevent"), result.Invalid.OriginalData)
}

func TestNewTransformation_Multiple(t *testing.T) {
	assert := assert.New(t)

	testCases := []struct {
		input        []byte
		key          string
		expectedPK   string
		expectedJSON []byte
	}{
		{input: transform.SnowplowTsv1, key: "key-1", expectedPK: "test-data1", expectedJSON: transform.SnowplowJSON1},
		{input: transform.SnowplowTsv2, key: "key-2", expectedPK: "test-data2", expectedJSON: transform.SnowplowJSON2},
		{input: transform.SnowplowTsv3, key: "key-3", expectedPK: "test-data3", expectedJSON: transform.SnowplowJSON3},
	}

	setPkToAppID, _ := sp_enriched.NewSpEnrichedSetPkFunction("app_id")
	transformMultiple := transform.NewTransformation(setPkToAppID, sp_enriched.SpEnrichedToJSON)

	for _, tc := range testCases {
		msg := &models.Message{Data: tc.input, PartitionKey: tc.key}
		result := transformMultiple(msg)

		assert.NotNil(result.Transformed)
		assert.Nil(result.Filtered)
		assert.Nil(result.Invalid)

		assert.JSONEq(string(tc.expectedJSON), string(result.Transformed.Data))
		assert.Equal(tc.expectedPK, result.Transformed.PartitionKey)
		assert.NotNil(result.Transformed.TimeTransformed)
		assert.Equal(tc.input, result.Transformed.OriginalData)
	}

	// Test with invalid Snowplow event
	msg := &models.Message{Data: []byte("not\ta\tsnowplow\tevent"), PartitionKey: "some-key4"}
	result := transformMultiple(msg)

	assert.Nil(result.Transformed)
	assert.Nil(result.Filtered)
	assert.NotNil(result.Invalid)
	assert.NotNil(result.Invalid.GetError())
	if result.Invalid.GetError() != nil {
		assert.Equal("intermediate state cannot be parsed as parsedEvent: cannot parse tsv event - wrong number of fields provided: 4", result.Invalid.GetError().Error())
	}
	assert.Equal([]byte("not\ta\tsnowplow\tevent"), result.Invalid.Data)
	assert.Equal("some-key4", result.Invalid.PartitionKey)
	assert.Equal([]byte("not\ta\tsnowplow\tevent"), result.Invalid.OriginalData)
}

func testfunc(message *models.Message, intermediateState any) (*models.Message, *models.Message, *models.Message, any) {
	return message, nil, nil, nil
}

func Benchmark_Transform_EnrichToJson(b *testing.B) {
	transformEnrichJSON := transform.NewTransformation(sp_enriched.SpEnrichedToJSON)
	for b.Loop() {
		msg := &models.Message{Data: transform.SnowplowTsv1, PartitionKey: "key"}
		transformEnrichJSON(msg)
	}
}

func Benchmark_Transform_Passthrough(b *testing.B) {
	transformPassthrough := transform.NewTransformation(testfunc)
	for b.Loop() {
		msg := &models.Message{Data: []byte("data"), PartitionKey: "key"}
		transformPassthrough(msg)
	}
}
