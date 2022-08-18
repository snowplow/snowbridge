// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"regexp"
	"testing"

	"github.com/dlclark/regexp2"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

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

func TestNewSpEnrichedFilterFunction(t *testing.T) {
	assert := assert.New(t)

	// Single value cases
	aidFilterFuncKeep, _ := NewSpEnrichedFilterFunction("app_id", "^test-data3$", 0)

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
	urlPrtFilterFuncKeep, _ := NewSpEnrichedFilterFunction("page_urlport", "^80$", 10)

	urlPrtKeepIn, urlPrtKeepOut, fail, _ := urlPrtFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, urlPrtKeepIn.Data)
	assert.Nil(urlPrtKeepOut)
	assert.Nil(fail)

	// Multiple value cases
	aidFilterFuncKeepWithMultiple, _ := NewSpEnrichedFilterFunction("app_id", "^someotherValue|test-data3$", 10)

	aidMultipleNegationFailedIn, aidMultipleKeepOut, fail3, _ := aidFilterFuncKeepWithMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidMultipleNegationFailedIn.Data)
	assert.Nil(aidMultipleKeepOut)
	assert.Nil(fail3)

	aidFilterFuncDiscardWithMultiple, _ := NewSpEnrichedFilterFunction("app_id", "^someotherValue|failThis$", 10)

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

	aidFilterFuncNegationKeep, _ := NewSpEnrichedFilterFunction("app_id", "^((?!someValue).)*$", 10)

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
	txnFilterFunctionAffirmation, _ := NewSpEnrichedFilterFunction("txn_id", "^something$", 10)

	nilAffirmationIn, nilAffirmationOut, fail8, _ := txnFilterFunctionAffirmation(&messageGood, nil)

	// nil doesn't match the regex and should be filtered out.
	assert.Nil(nilAffirmationIn)
	assert.Equal(snowplowTsv3, nilAffirmationOut.Data)
	assert.Nil(fail8)

	txnFilterFunctionNegation, _ := NewSpEnrichedFilterFunction("txn_id", "^((?!something).)*$", 10)

	nilNegationIn, nilNegationOut, fail8, _ := txnFilterFunctionNegation(&messageGood, nil)

	// nil DOES match the negative lookup - it doesn't contain 'something'. So should be kept.
	assert.Equal(snowplowTsv3, nilNegationIn.Data)
	assert.Nil(nilNegationOut)
	assert.Nil(fail8)

	fieldNotExistsFilter, _ := NewSpEnrichedFilterFunction("nothing", "", 10)

	notExistsIn, notExistsOut, notExistsFail, _ := fieldNotExistsFilter(&messageGood, nil)

	assert.Nil(notExistsIn)
	assert.Nil(notExistsOut)
	assert.NotNil(notExistsFail)
}

func TestNewSpEnrichedFilterFunctionContext(t *testing.T) {
	assert := assert.New(t)

	// The relevant data in messageGood looks like this: "test1":{"test2":[{"test3":"testValue"}]

	// context filter success
	contextFuncKeep, _ := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", 10)

	contextKeepIn, contextKeepOut, fail9, _ := contextFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, contextKeepIn.Data)
	assert.Nil(contextKeepOut)
	assert.Nil(fail9)

	// The relevant data in messageGoodInt looks like this: "test1":{"test2":[{"test3":1}]

	// context filter success (integer value)
	contextFuncKeep, _ = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^1$", 10)

	contextKeepIn, contextKeepOut, fail9, _ = contextFuncKeep(&messageGoodInt, nil)

	assert.Equal(snowplowTsv4, contextKeepIn.Data)
	assert.Nil(contextKeepOut)
	assert.Nil(fail9)

	// context filter wrong path
	contextFuncKeep, _ = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].test3", "^testValue$", 10)

	contextKeepIn, contextKeepOut, fail9, _ = contextFuncKeep(&messageGood, nil)

	assert.Nil(contextKeepIn)
	assert.Equal(snowplowTsv3, contextKeepOut.Data)
	assert.Nil(fail9)
}

func TestNewSpEnrichedFilterFunctionUnstructEvent(t *testing.T) {
	assert := assert.New(t)

	// event filter success, filtered event name
	eventFilterFuncKeep, _ := NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^item41$", 10)

	eventKeepIn, eventKeepOut, fail10, _ := eventFilterFuncKeep(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventKeepIn.Data)
	assert.Nil(eventKeepOut)
	assert.Nil(fail10)

	// event filter success, filtered event name, no event ver
	eventFilterFuncKeep, _ = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "", "sku", "^item41$", 10)

	eventKeepIn, eventKeepOut, fail10, _ = eventFilterFuncKeep(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventKeepIn.Data)
	assert.Nil(eventKeepOut)
	assert.Nil(fail10)

	// event filter failure, wrong event name
	eventFilterFuncKeep, _ = NewSpEnrichedFilterFunctionUnstructEvent("wrong_name", "", "sku", "^item41$", 10)

	eventKeepIn, eventKeepOut, fail11, _ := eventFilterFuncKeep(&messageWithUnstructEvent, nil)

	assert.Nil(eventKeepIn)
	assert.Equal(snowplowTsv1, eventKeepOut.Data)
	assert.Nil(fail11)

	// event filter failure, field not found
	eventFilterFuncKeep, _ = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "", "ska", "item41", 10)

	eventNoFieldIn, eventNoFieldOut, fail12, _ := eventFilterFuncKeep(&messageWithUnstructEvent, nil)

	assert.Nil(eventNoFieldIn)
	assert.Equal(snowplowTsv1, eventNoFieldOut.Data)
	assert.Nil(fail12)

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

	filterFunc, _ := NewSpEnrichedFilterFunction("app_id", "^test-data1$", 10)

	filter1 := NewTransformation(filterFunc)
	filter1Res := filter1(Messages)

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

	filterFunc2, _ := NewSpEnrichedFilterFunction("app_id", "^test-data1|test-data2$", 10)

	filter2 := NewTransformation(filterFunc2)
	filter2Res := filter2(Messages)

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
	filter3Res := filter3(Messages)

	assert.Equal(len(expectedFilter3), len(filter3Res.Result))
	assert.Equal(1, len(filter3Res.Invalid))

}

func TestEvaluateSpEnrichedFilter(t *testing.T) {
	assert := assert.New(t)

	regex, err := regexp2.Compile("^yes$", 0)
	if err != nil {
		panic(err)
	}

	valuesFound := []interface{}{"NO", "maybe", "yes"}
	assert.True(evaluateSpEnrichedFilter(regex, valuesFound))

	valuesFound2 := []interface{}{"NO", "maybe", "nope", nil}
	assert.False(evaluateSpEnrichedFilter(regex, valuesFound2))

	regexInt, err := regexp2.Compile("^123$", 0)
	if err != nil {
		panic(err)
	}

	valuesFound3 := []interface{}{123, "maybe", "nope", nil}
	assert.True(evaluateSpEnrichedFilter(regexInt, valuesFound3))

	// This asserts that when any element of the input is nil, we assert against empty string.
	// It exists to ensure we don't evaluate against the string `<nil>` since we're naively casting values to string.
	regexNil, err := regexp2.Compile("^$", 0)
	if err != nil {
		panic(err)
	}

	assert.True(evaluateSpEnrichedFilter(regexNil, []interface{}{nil}))

	// just to make sure the regex only matches empty:
	assert.False(evaluateSpEnrichedFilter(regexNil, []interface{}{"a"}))

	// These tests ensures that when getters return a nil slice, we're still asserting against the empty value.
	// This is important since we have negative lookaheads.

	assert.True(evaluateSpEnrichedFilter(regexNil, nil))

	// negative lookahead:
	regexNegative, err := regexp2.Compile("^((?!failThis).)*$", 0)
	if err != nil {
		panic(err)
	}

	assert.True(evaluateSpEnrichedFilter(regexNegative, nil))
}

func TestMakeBaseValueGetter(t *testing.T) {
	assert := assert.New(t)

	// simple app ID
	appIDGetter := makeBaseValueGetter("app_id")

	res, err := appIDGetter(spTsv3Parsed)

	assert.Equal([]interface{}{"test-data3"}, res)
	assert.Nil(err)

	nonExistentFieldGetter := makeBaseValueGetter("nope")

	res2, err2 := nonExistentFieldGetter(spTsv3Parsed)

	assert.Nil(res2)
	assert.NotNil(err2)
	if err2 != nil {
		assert.Equal("Key nope not a valid atomic field", err2.Error())
	}
	// TODO: currently we'll only hit this error while processing data. Ideally we should hit it on startup.
}

func TestMakeContextValueGetter(t *testing.T) {
	assert := assert.New(t)

	contextGetter := makeContextValueGetter("contexts_nl_basjes_yauaa_context_1", []interface{}{"test1", "test2", 0, "test3"})

	res, err := contextGetter(spTsv3Parsed)

	assert.Equal([]interface{}{"testValue"}, res)
	assert.Nil(err)

	res2, err2 := contextGetter(spTsv1Parsed)

	// If the path doesn't exist, we shoud return nil, nil.
	assert.Nil(res2)
	assert.Nil(err2)

	contextGetterArray := makeContextValueGetter("contexts_com_acme_just_ints_1", []interface{}{"integerField"})

	res3, err3 := contextGetterArray(spTsv1Parsed)

	assert.Equal([]interface{}{float64(0), float64(1), float64(2)}, res3)
	assert.Nil(err3)
}

func TestMakeUnstructValueGetter(t *testing.T) {
	assert := assert.New(t)

	re1 := regexp.MustCompile("1-*-*")

	unstructGetter := makeUnstructValueGetter("add_to_cart", re1, []interface{}{"sku"})

	res, err := unstructGetter(spTsv1Parsed)

	assert.Equal([]interface{}{"item41"}, res)
	assert.Nil(err)

	unstructGetterWrongPath := makeUnstructValueGetter("add_to_cart", re1, []interface{}{"notSku"})

	// If it's not in the event, both should be nil
	res2, err2 := unstructGetterWrongPath(spTsv1Parsed)

	assert.Nil(res2)
	assert.Nil(err2)

	// test that wrong schema version behaves appropriately (return nil nil)
	re2 := regexp.MustCompile("2-*-*")

	unstructWrongSchemaGetter := makeUnstructValueGetter("add_to_cart", re2, []interface{}{"sku"})

	res3, err3 := unstructWrongSchemaGetter(spTsv1Parsed)

	assert.Nil(res3)
	assert.Nil(err3)

	// test that not specifying a version behaves appropriately (accepts all versions)
	re3 := regexp.MustCompile("")

	unstructAnyVersionGetter := makeUnstructValueGetter("add_to_cart", re3, []interface{}{"sku"})

	res4, err4 := unstructAnyVersionGetter(spTsv1Parsed)

	assert.Equal([]interface{}{"item41"}, res4)
	assert.Nil(err4)

	// test that wrong event name behaves appropriately (return nil nil)

	unstructWrongEvnetName := makeUnstructValueGetter("not_add_to_cart_at_all", re3, []interface{}{"sku"})

	res5, err5 := unstructWrongEvnetName(spTsv1Parsed)

	assert.Nil(res5)
	assert.Nil(err5)
}

func BenchmarkBaseFieldFilter(b *testing.B) {
	var messageGood = models.Message{
		Data:         snowplowTsv3,
		PartitionKey: "some-key",
	}
	aidFilterFuncKeep, _ := NewSpEnrichedFilterFunction("app_id", "^test-data3$", 0)

	aidFilterFuncNegationKeep, _ := NewSpEnrichedFilterFunction("app_id", "^((?!failThis).)*$", 10)

	for i := 0; i < b.N; i++ {

		aidFilterFuncKeep(&messageGood, nil)
		aidFilterFuncNegationKeep(&messageGood, nil)
	}
}

func BenchmarkContextFilterNew(b *testing.B) {
	var messageGood = models.Message{
		Data:         snowplowTsv3,
		PartitionKey: "some-key",
	}

	contextFuncAffirm, _ := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", 10)
	contextFuncNegate, _ := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^((?!failThis).)*$", 10)

	for i := 0; i < b.N; i++ {
		contextFuncAffirm(&messageGood, nil)
		contextFuncNegate(&messageGood, nil)
	}
}

func BenchmarkUnstructFilterNew(b *testing.B) {
	var messageGood = models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}

	unstructFilterFuncAffirm, _ := NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^item41$", 10)
	unstructFilterFuncNegate, _ := NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^((?!failThis).)*$", 10)

	for i := 0; i < b.N; i++ {
		unstructFilterFuncAffirm(&messageGood, nil)
		unstructFilterFuncNegate(&messageGood, nil)

	}
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
