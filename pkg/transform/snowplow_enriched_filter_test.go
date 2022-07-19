// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestNewSpEnrichedFilterFunction(t *testing.T) {
	assert := assert.New(t)

	var messageGood = models.Message{
		Data:         snowplowTsv3,
		PartitionKey: "some-key",
	}

	var messageGoodInt = models.Message{
		Data:         snowplowTsv4,
		PartitionKey: "some-key",
	}

	var messageWithUnstructEvent = models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}

	// Single value cases
	aidFilterFuncKeep, _ := NewSpEnrichedFilterFunction("app_id", "test-data3", 0)

	aidKeepIn, aidKeepOut, fail, _ := aidFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidKeepIn.Data)
	assert.Nil(aidKeepOut)
	assert.Nil(fail)

	aidFilterFuncDiscard, _ := NewSpEnrichedFilterFunction("app_id", "failThis", 10)

	aidDiscardIn, aidDiscardOut, fail2, _ := aidFilterFuncDiscard(&messageGood, nil)

	assert.Nil(aidDiscardIn)
	assert.Equal(snowplowTsv3, aidDiscardOut.Data)
	assert.Nil(fail2)

	// int value
	urlPrtFilterFuncKeep, _ := NewSpEnrichedFilterFunction("page_urlport", "80", 10)

	urlPrtKeepIn, urlPrtKeepOut, fail, _ := urlPrtFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, urlPrtKeepIn.Data)
	assert.Nil(urlPrtKeepOut)
	assert.Nil(fail)

	// Multiple value cases
	aidFilterFuncKeepWithMultiple, _ := NewSpEnrichedFilterFunction("app_id", "someotherValue|test-data3", 10)

	aidMultipleNegationFailedIn, aidMultipleKeepOut, fail3, _ := aidFilterFuncKeepWithMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidMultipleNegationFailedIn.Data)
	assert.Nil(aidMultipleKeepOut)
	assert.Nil(fail3)

	aidFilterFuncDiscardWithMultiple, _ := NewSpEnrichedFilterFunction("app_id", "someotherValue|failThis", 10)

	aidNegationMultipleIn, aidMultipleDiscardOut, fail3, _ := aidFilterFuncDiscardWithMultiple(&messageGood, nil)

	assert.Nil(aidNegationMultipleIn)
	assert.Equal(snowplowTsv3, aidMultipleDiscardOut.Data)
	assert.Nil(fail3)

	// Single value negation cases

	aidFilterFuncNegationDiscard, _ := NewSpEnrichedFilterFunction("app_id", "^((?!test-data3).)*$", 10)

	aidNegationIn, aidNegationOut, fail4, _ := aidFilterFuncNegationDiscard(&messageGood, nil)

	assert.Nil(aidNegationIn)
	assert.Equal(snowplowTsv3, aidNegationOut.Data)
	assert.Nil(fail4)

	aidFilterFuncNegationKeep, _ := NewSpEnrichedFilterFunction("app_id", "^((?!failThis).)*$", 10)

	aidNegationFailedIn, aidNegationFailedOut, fail5, _ := aidFilterFuncNegationKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidNegationFailedIn.Data)
	assert.Nil(aidNegationFailedOut)
	assert.Nil(fail5)

	// Multiple value negation cases
	aidFilterFuncNegationDiscardMultiple, _ := NewSpEnrichedFilterFunction("app_id", "^((?!someotherValue|test-data1|test-data2|test-data3).)*$", 10)

	aidNegationMultipleIn, aidNegationMultipleOut, fail6, _ := aidFilterFuncNegationDiscardMultiple(&messageGood, nil)

	assert.Nil(aidNegationMultipleIn)
	assert.Equal(snowplowTsv3, aidNegationMultipleOut.Data)
	assert.Nil(fail6)

	aidFilterFuncNegationKeptMultiple, _ := NewSpEnrichedFilterFunction("app_id", "^((?!someotherValue|failThis).)*$", 10)

	aidMultipleNegationFailedIn, aidMultipleNegationFailedOut, fail7, _ := aidFilterFuncNegationKeptMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidMultipleNegationFailedIn.Data)
	assert.Nil(aidMultipleNegationFailedOut)
	assert.Nil(fail7)

	// Filters on a nil field
	txnFilterFunctionAffirmation, _ := NewSpEnrichedFilterFunction("txn_id", "something", 10)

	nilAffirmationIn, nilAffirmationOut, fail8, _ := txnFilterFunctionAffirmation(&messageGood, nil)

	assert.Nil(nilAffirmationIn)
	assert.Equal(snowplowTsv3, nilAffirmationOut.Data)
	assert.Nil(fail8)

	txnFilterFunctionNegation, _ := NewSpEnrichedFilterFunction("txn_id", "^((?!something).)*$", 10)

	nilNegationIn, nilNegationOut, fail8, _ := txnFilterFunctionNegation(&messageGood, nil)

	assert.Nil(nilNegationIn)
	assert.Equal(snowplowTsv3, nilNegationOut.Data)
	assert.Nil(fail8)

	// context filter success
	contextFuncKeep, _ := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1.test1.test2[0].test3", "testValue", 10)

	contextKeepIn, contextKeepOut, fail9, _ := contextFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, contextKeepIn.Data)
	assert.Nil(contextKeepOut)
	assert.Nil(fail9)

	// context filter success (integer value)
	contextFuncKeep, _ = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1.test1.test2[0].test3", "1", 10)

	contextKeepIn, contextKeepOut, fail9, _ = contextFuncKeep(&messageGoodInt, nil)

	assert.Equal(snowplowTsv4, contextKeepIn.Data)
	assert.Nil(contextKeepOut)
	assert.Nil(fail9)

	// context filter failure
	contextFuncKeep, _ = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_2.test1.test2[0].test3", "testValue", 10)

	contextKeepIn, contextKeepOut, fail9, _ = contextFuncKeep(&messageGood, nil)

	assert.Nil(contextKeepIn)
	assert.Equal(snowplowTsv3, contextKeepOut.Data)
	assert.Nil(fail9)

	// event filter success, filtered event name
	eventFilterFunCkeep, _ := NewSpEnrichedFilterFunctionUnstructEvent("unstruct_event_add_to_cart_1.sku", "item41", 10)

	eventKeepIn, eventKeepOut, fail10, _ := eventFilterFunCkeep(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventKeepIn.Data)
	assert.Nil(eventKeepOut)
	assert.Nil(fail10)

	// event filter success, filtered event name, no event ver
	eventFilterFunCkeep, _ = NewSpEnrichedFilterFunctionUnstructEvent("unstruct_event_add_to_cart.sku", "item41", 10)

	eventKeepIn, eventKeepOut, fail10, _ = eventFilterFunCkeep(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventKeepIn.Data)
	assert.Nil(eventKeepOut)
	assert.Nil(fail10)

	// event filter failure, wrong event name
	eventFilterFunCkeep, _ = NewSpEnrichedFilterFunctionUnstructEvent("unstruct_event_wrong_name.sku", "item41", 10)

	eventKeepIn, eventKeepOut, fail11, _ := eventFilterFunCkeep(&messageWithUnstructEvent, nil)

	assert.Nil(eventKeepIn)
	assert.Equal(snowplowTsv1, eventKeepOut.Data)
	assert.Nil(fail11)

	// event filter failure, field not found
	eventFilterFunCkeep, _ = NewSpEnrichedFilterFunctionUnstructEvent("unstruct_event_add_to_cart.ska", "item41", 10)

	eventNoFieldIn, eventNoFieldOut, fail12, _ := eventFilterFunCkeep(&messageWithUnstructEvent, nil)

	assert.Nil(eventNoFieldIn)
	assert.Nil(eventNoFieldOut)
	assert.NotNil(fail12)
}

func TestSpEnrichedFilterFunction_Slice(t *testing.T) {
	assert := assert.New(t)

	var filter1Kept = []*models.Message{
		{
			Data:         snowplowTsv1,
			PartitionKey: "some-key",
		},
	}

	var filter1Discarded = []*models.Message{
		{
			Data:         snowplowTsv2,
			PartitionKey: "some-key1",
		},
		{
			Data:         snowplowTsv3,
			PartitionKey: "some-key2",
		},
	}

	filterFunc, _ := NewSpEnrichedFilterFunction("app_id", "test-data1", 10)

	filter1 := NewTransformation(filterFunc)
	filter1Res := filter1(messages)

	assert.Equal(len(filter1Kept), len(filter1Res.Result))
	assert.Equal(len(filter1Discarded), len(filter1Res.Filtered))
	assert.Equal(1, len(filter1Res.Invalid))

	var filter2Kept = []*models.Message{
		{
			Data:         snowplowTsv1,
			PartitionKey: "some-key",
		},
		{
			Data:         snowplowTsv2,
			PartitionKey: "some-key1",
		},
	}

	var filter2Discarded = []*models.Message{

		{
			Data:         snowplowTsv3,
			PartitionKey: "some-key2",
		},
	}

	filterFunc2, _ := NewSpEnrichedFilterFunction("app_id", "test-data1|test-data2", 10)

	filter2 := NewTransformation(filterFunc2)
	filter2Res := filter2(messages)

	assert.Equal(len(filter2Kept), len(filter2Res.Result))
	assert.Equal(len(filter2Discarded), len(filter2Res.Filtered))
	assert.Equal(1, len(filter2Res.Invalid))

	var expectedFilter3 = []*models.Message{
		{
			Data:         snowplowTsv3,
			PartitionKey: "some-key3",
		},
	}

	filterFunc3, _ := NewSpEnrichedFilterFunction("app_id", "^((?!test-data1|test-data2).)*$", 10)

	filter3 := NewTransformation(filterFunc3)
	filter3Res := filter3(messages)

	assert.Equal(len(expectedFilter3), len(filter3Res.Result))
	assert.Equal(1, len(filter3Res.Invalid))

}

func TestEvaluateSpEnrichedFilter(t *testing.T) {
	assert := assert.New(t)

	valuesFound := []interface{}{"NO", "maybe", "yes"}
	assert.True(evaluateSpEnrichedFilter(valuesFound, "yes", 10))

	valuesFound = []interface{}{"NO", "maybe", "nope"}
	assert.False(evaluateSpEnrichedFilter(valuesFound, "yes", 10))
}
