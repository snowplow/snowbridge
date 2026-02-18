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

package filter

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/transform"
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

	// Process each message individually and aggregate results
	var totalResult, totalFiltered, totalInvalid []*models.Message
	for _, msg := range transform.Messages {
		res := filter1(msg)
		if res.Transformed != nil {
			totalResult = append(totalResult, res.Transformed)
		}
		if res.Filtered != nil {
			totalFiltered = append(totalFiltered, res.Filtered)
		}
		if res.Invalid != nil {
			totalInvalid = append(totalInvalid, res.Invalid)
		}
	}

	assert.Equal(len(filter1Kept), len(totalResult))
	assert.Equal(len(filter1Discarded), len(totalFiltered))
	assert.Equal(1, len(totalInvalid))

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

	// Process each message individually and aggregate results
	var totalResult2, totalFiltered2, totalInvalid2 []*models.Message
	for _, msg := range transform.Messages {
		res := filter2(msg)
		if res.Transformed != nil {
			totalResult2 = append(totalResult2, res.Transformed)
		}
		if res.Filtered != nil {
			totalFiltered2 = append(totalFiltered2, res.Filtered)
		}
		if res.Invalid != nil {
			totalInvalid2 = append(totalInvalid2, res.Invalid)
		}
	}

	assert.Equal(len(filter2Kept), len(totalResult2))
	assert.Equal(len(filter2Discarded), len(totalFiltered2))
	assert.Equal(1, len(totalInvalid2))

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

	// Process each message individually and aggregate results
	var totalResult3, totalFiltered3, totalInvalid3 []*models.Message
	for _, msg := range transform.Messages {
		res := filter3(msg)
		if res.Transformed != nil {
			totalResult3 = append(totalResult3, res.Transformed)
		}
		if res.Filtered != nil {
			totalFiltered3 = append(totalFiltered3, res.Filtered)
		}
		if res.Invalid != nil {
			totalInvalid3 = append(totalInvalid3, res.Invalid)
		}
	}

	assert.Equal(len(expectedFilter3), len(totalResult3))
	assert.Equal(2, len(totalFiltered3))
	assert.Equal(1, len(totalInvalid3))

}

func TestEvaluateSpEnrichedFilter(t *testing.T) {
	assert := assert.New(t)

	regex, err := regexp.Compile("^yes$")
	if err != nil {
		panic(err)
	}

	valuesFound := []any{"NO", "maybe", "yes"}
	assert.True(evaluateSpEnrichedFilter(regex, valuesFound))

	valuesFound2 := []any{"NO", "maybe", "nope", nil}
	assert.False(evaluateSpEnrichedFilter(regex, valuesFound2))

	regexInt, err := regexp.Compile("^123$")
	if err != nil {
		panic(err)
	}

	valuesFound3 := []any{123, "maybe", "nope", nil}
	assert.True(evaluateSpEnrichedFilter(regexInt, valuesFound3))

	// This asserts that when any element of the input is nil, we assert against empty string.
	// It exists to ensure we don't evaluate against the string `<nil>` since we're naively casting values to string.
	regexNil, err := regexp.Compile("^$")
	if err != nil {
		panic(err)
	}

	assert.True(evaluateSpEnrichedFilter(regexNil, []any{nil}))

	// just to make sure the regex only matches empty:
	assert.False(evaluateSpEnrichedFilter(regexNil, []any{"a"}))

	// These tests ensures that when getters return a nil slice, we're still asserting against the empty value.
	// This is important since we have negative lookaheads.

	assert.True(evaluateSpEnrichedFilter(regexNil, nil))
}

func TestParsePathToArguments(t *testing.T) {
	assert := assert.New(t)

	// Common case
	path1, err1 := parsePathToArguments("test1[123].test2[1].test3")
	expectedPath1 := []any{"test1", 123, "test2", 1, "test3"}

	assert.Equal(expectedPath1, path1)
	assert.Nil(err1)

	// Success edge case - field names with different character
	path2, err2 := parsePathToArguments("test-1.test_2[1].test$3")
	expectedPath2 := []any{"test-1", "test_2", 1, "test$3"}

	assert.Equal(expectedPath2, path2)
	assert.Nil(err2)

	// Success edge case - field name is stringified int
	path3, err3 := parsePathToArguments("123.456[1].789")
	expectedPath3 := []any{"123", "456", 1, "789"}

	assert.Equal(expectedPath3, path3)
	assert.Nil(err3)

	// Success edge case - nested arrays
	path4, err4 := parsePathToArguments("test1.test2[1][2].test3")
	expectedPath4 := []any{"test1", "test2", 1, 2, "test3"}

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
