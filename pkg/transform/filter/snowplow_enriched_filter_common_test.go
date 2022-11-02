// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package filter

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
)

var messageGood = models.Message{
	Data:         transform.SnowplowTsv3,
	PartitionKey: "some-key",
}

var messageGoodInt = models.Message{
	Data:         transform.SnowplowTsv4,
	PartitionKey: "some-key",
}

var messageWithUnstructEvent = models.Message{
	Data:         transform.SnowplowTsv1,
	PartitionKey: "some-key",
}

func TestFilteringSlice(t *testing.T) {
	assert := assert.New(t)

	var filter1Kept = []*models.Message{
		{
			Data:         transform.SnowplowTsv1,
			PartitionKey: "some-key",
		},
	}

	var filter1Discarded = []*models.Message{
		{
			Data:         transform.SnowplowTsv2,
			PartitionKey: "some-key1",
		},
		{
			Data:         transform.SnowplowTsv3,
			PartitionKey: "some-key2",
		},
	}

	filterFunc, err := NewAtomicFilterFunction("app_id", "^test-data1$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	filter1 := transform.NewTransformation(filterFunc)
	filter1Res := filter1(transform.Messages)

	assert.Equal(len(filter1Kept), len(filter1Res.Result))
	assert.Equal(len(filter1Discarded), len(filter1Res.Filtered))
	assert.Equal(1, len(filter1Res.Invalid))

	var filter2Kept = []*models.Message{
		{
			Data:         transform.SnowplowTsv1,
			PartitionKey: "some-key",
		},
		{
			Data:         transform.SnowplowTsv2,
			PartitionKey: "some-key1",
		},
	}

	var filter2Discarded = []*models.Message{

		{
			Data:         transform.SnowplowTsv3,
			PartitionKey: "some-key2",
		},
	}

	filterFunc2, err := NewAtomicFilterFunction("app_id", "^test-data1|test-data2$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	filter2 := transform.NewTransformation(filterFunc2)
	filter2Res := filter2(transform.Messages)

	assert.Equal(len(filter2Kept), len(filter2Res.Result))
	assert.Equal(len(filter2Discarded), len(filter2Res.Filtered))
	assert.Equal(1, len(filter2Res.Invalid))

	var expectedFilter3 = []*models.Message{
		{
			Data:         transform.SnowplowTsv3,
			PartitionKey: "some-key3",
		},
	}

	filterFunc3, err := NewAtomicFilterFunction("app_id", "^(test-data1|test-data2)$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	filter3 := transform.NewTransformation(filterFunc3)
	filter3Res := filter3(transform.Messages)

	assert.Equal(len(expectedFilter3), len(filter3Res.Result))
	assert.Equal(1, len(filter3Res.Invalid))

}

func TestEvaluateSpEnrichedFilter(t *testing.T) {
	assert := assert.New(t)

	regex, err := regexp.Compile("^yes$")
	if err != nil {
		panic(err)
	}

	valuesFound := []interface{}{"NO", "maybe", "yes"}
	assert.True(evaluateSpEnrichedFilter(regex, valuesFound))

	valuesFound2 := []interface{}{"NO", "maybe", "nope", nil}
	assert.False(evaluateSpEnrichedFilter(regex, valuesFound2))

	regexInt, err := regexp.Compile("^123$")
	if err != nil {
		panic(err)
	}

	valuesFound3 := []interface{}{123, "maybe", "nope", nil}
	assert.True(evaluateSpEnrichedFilter(regexInt, valuesFound3))

	// This asserts that when any element of the input is nil, we assert against empty string.
	// It exists to ensure we don't evaluate against the string `<nil>` since we're naively casting values to string.
	regexNil, err := regexp.Compile("^$")
	if err != nil {
		panic(err)
	}

	assert.True(evaluateSpEnrichedFilter(regexNil, []interface{}{nil}))

	// just to make sure the regex only matches empty:
	assert.False(evaluateSpEnrichedFilter(regexNil, []interface{}{"a"}))

	// These tests ensures that when getters return a nil slice, we're still asserting against the empty value.
	// This is important since we have negative lookaheads.

	assert.True(evaluateSpEnrichedFilter(regexNil, nil))
}

func TestParsePathToArguments(t *testing.T) {
	assert := assert.New(t)

	// Common case
	path1, err1 := parsePathToArguments("test1[123].test2[1].test3")
	expectedPath1 := []interface{}{"test1", 123, "test2", 1, "test3"}

	assert.Equal(expectedPath1, path1)
	assert.Nil(err1)

	// Success edge case - field names with different character
	path2, err2 := parsePathToArguments("test-1.test_2[1].test$3")
	expectedPath2 := []interface{}{"test-1", "test_2", 1, "test$3"}

	assert.Equal(expectedPath2, path2)
	assert.Nil(err2)

	// Success edge case - field name is stringified int
	path3, err3 := parsePathToArguments("123.456[1].789")
	expectedPath3 := []interface{}{"123", "456", 1, "789"}

	assert.Equal(expectedPath3, path3)
	assert.Nil(err3)

	// Success edge case - nested arrays
	path4, err4 := parsePathToArguments("test1.test2[1][2].test3")
	expectedPath4 := []interface{}{"test1", "test2", 1, 2, "test3"}

	assert.Equal(expectedPath4, path4)
	assert.Nil(err4)

	// Failure edge case - unmatched brace in path
	// We are validating for this and failing at startup, with the assumption that it must be misconfiguration.
	path5, err5 := parsePathToArguments("test1.test[2.test3")

	assert.Nil(path5)
	assert.NotNil(err5)
	if err5 != nil {
		assert.Equal("unmatched brace in path: test1.test[2.test3", err5.Error())
	}
}
