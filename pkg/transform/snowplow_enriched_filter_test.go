// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"fmt"
	"regexp"
	"testing"

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
	aidFilterFuncKeep, err := NewSpEnrichedFilterFunction("app_id", "^test-data3$", "keep")
	if err != nil {
		panic(err)
	}

	aidKeepIn, aidKeepOut, fail, _ := aidFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidKeepIn.Data)
	assert.Nil(aidKeepOut)
	assert.Nil(fail)

	aidFilterFuncDiscard, err := NewSpEnrichedFilterFunction("app_id", "failThis", "keep")
	if err != nil {
		panic(err)
	}

	aidDiscardIn, aidDiscardOut, fail2, _ := aidFilterFuncDiscard(&messageGood, nil)

	assert.Nil(aidDiscardIn)
	assert.Equal(snowplowTsv3, aidDiscardOut.Data)
	assert.Nil(fail2)

	// int value
	urlPrtFilterFuncKeep, err := NewSpEnrichedFilterFunction("page_urlport", "^80$", "keep")
	if err != nil {
		panic(err)
	}

	urlPrtKeepIn, urlPrtKeepOut, fail, _ := urlPrtFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, urlPrtKeepIn.Data)
	assert.Nil(urlPrtKeepOut)
	assert.Nil(fail)

	// Multiple value cases
	aidFilterFuncKeepWithMultiple, err := NewSpEnrichedFilterFunction("app_id", "^someotherValue|test-data3$", "keep")
	if err != nil {
		panic(err)
	}

	aidMultipleIn, aidMultipleOut, fail, _ := aidFilterFuncKeepWithMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidMultipleIn.Data)
	assert.Nil(aidMultipleOut)
	assert.Nil(fail)

	aidFilterFuncDiscardWithMultiple, _ := NewSpEnrichedFilterFunction("app_id", "^someotherValue|failThis$", "keep")
	if err != nil {
		panic(err)
	}

	aidNoneOfMultipleIn, aidNoneOfMultipleOut, fail, _ := aidFilterFuncDiscardWithMultiple(&messageGood, nil)

	assert.Nil(aidNoneOfMultipleIn)
	assert.Equal(snowplowTsv3, aidNoneOfMultipleOut.Data)
	assert.Nil(fail)

	// Single value negation cases
	aidFilterFuncNegationDiscard, err := NewSpEnrichedFilterFunction("app_id", "^test-data3", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidNegationIn, aidNegationOut, fail, _ := aidFilterFuncNegationDiscard(&messageGood, nil)

	assert.Nil(aidNegationIn)
	assert.Equal(snowplowTsv3, aidNegationOut.Data)
	assert.Nil(fail)

	aidFilterFuncNegationKeep, err := NewSpEnrichedFilterFunction("app_id", "^someValue", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidNegationFailedIn, aidNegationFailedOut, fail, _ := aidFilterFuncNegationKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidNegationFailedIn.Data)
	assert.Nil(aidNegationFailedOut)
	assert.Nil(fail)

	// Multiple value negation cases
	aidFilterFuncNegationDiscardMultiple, err := NewSpEnrichedFilterFunction("app_id", "^(someotherValue|test-data1|test-data2|test-data3)", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidNoneOfMultipleIn, aidNegationMultipleOut, fail, _ := aidFilterFuncNegationDiscardMultiple(&messageGood, nil)

	assert.Nil(aidNoneOfMultipleIn)
	assert.Equal(snowplowTsv3, aidNegationMultipleOut.Data)
	assert.Nil(fail)

	aidFilterFuncNegationKeptMultiple, err := NewSpEnrichedFilterFunction("app_id", "^(someotherValue|failThis)$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidMultipleIn, aidMultipleNegationFailedOut, fail, _ := aidFilterFuncNegationKeptMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidMultipleIn.Data)
	assert.Nil(aidMultipleNegationFailedOut)
	assert.Nil(fail)

	// Filters on a nil field
	txnFilterFunctionAffirmation, err := NewSpEnrichedFilterFunction("txn_id", "^something$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	nilAffirmationIn, nilAffirmationOut, fail, _ := txnFilterFunctionAffirmation(&messageGood, nil)

	// nil doesn't match the regex and should be filtered out.
	assert.Nil(nilAffirmationIn)
	assert.Equal(snowplowTsv3, nilAffirmationOut.Data)
	assert.Nil(fail)

	txnFilterFunctionNegation, err := NewSpEnrichedFilterFunction("txn_id", "^something", "drop")
	if err != nil {
		fmt.Println(err)
	}

	nilNegationIn, nilNegationOut, fail, _ := txnFilterFunctionNegation(&messageGood, nil)

	// nil DOES match the negative lookup - it doesn't contain 'something'. So should be kept.
	assert.Equal(snowplowTsv3, nilNegationIn.Data)
	assert.Nil(nilNegationOut)
	assert.Nil(fail)

	fieldNotExistsFilter, err := NewSpEnrichedFilterFunction("nothing", "", "keep")
	if err != nil {
		fmt.Println(err)
	}

	notExistsIn, notExistsOut, notExistsFail, _ := fieldNotExistsFilter(&messageGood, nil)

	assert.Nil(notExistsIn)
	assert.Nil(notExistsOut)
	assert.NotNil(notExistsFail)
}

func TestNewSpEnrichedFilterFunctionContext(t *testing.T) {
	assert := assert.New(t)

	// The relevant data in messageGood looks like this: "test1":{"test2":[{"test3":"testValue"}]

	// context filter success
	contextFilterFunc, err := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ := contextFilterFunc(&messageGood, nil)

	assert.Equal(snowplowTsv3, contextIn.Data)
	assert.Nil(contextOut)
	assert.Nil(fail)

	// same, with 'drop'
	contextFilterFunc, err = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Nil(contextIn)
	assert.Equal(snowplowTsv3, contextOut.Data)
	assert.Nil(fail)

	// The relevant data in messageGoodInt looks like this: "test1":{"test2":[{"test3":1}]

	// context filter success (integer value)
	contextFilterFunc, err = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^1$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGoodInt, nil)

	assert.Equal(snowplowTsv4, contextIn.Data)
	assert.Nil(contextOut)
	assert.Nil(fail)

	// same, with 'drop'
	contextFilterFunc, err = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^1$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGoodInt, nil)

	assert.Nil(contextIn)
	assert.Equal(snowplowTsv4, contextOut.Data)
	assert.Nil(fail)

	// context filter wrong context name
	contextFilterFunc, err = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].test3", "^testValue$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Nil(contextIn)
	assert.Equal(snowplowTsv3, contextOut.Data)
	assert.Nil(fail)

	// Context filter path doesn't exist

	// This configuration is 'keep values that match "^testValue$"'. If the path is wrong, tha value is empty, which doesn't match that regex - so it should be filtered out.
	contextFilterFunc, err = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].nothingHere", "^testValue$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Nil(contextIn)
	assert.Equal(snowplowTsv3, contextOut.Data)
	assert.Nil(fail)

	// This says 'drop values that match "^testValue$"'. If the path is wrong, the value is empty, which doesn't match that regex - so it should be kept.
	contextFilterFunc, err = NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].nothingHere", "^testValue$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Equal(snowplowTsv3, contextIn.Data)
	assert.Nil(contextOut)
	assert.Nil(fail)
}

func TestNewSpEnrichedFilterFunctionUnstructEvent(t *testing.T) {
	assert := assert.New(t)

	// event filter success, filtered event name
	eventFilterFunc, err := NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^item41$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ := eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventIn.Data)
	assert.Nil(eventOut)
	assert.Nil(fail)

	// same, with 'drop'
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^item41$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventIn)
	assert.Equal(snowplowTsv1, eventOut.Data)
	assert.Nil(fail)

	// event filter success, filtered event name, no event version
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "", "sku", "^item41$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventIn.Data)
	assert.Nil(eventOut)
	assert.Nil(fail)

	// same with 'drop'
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "", "sku", "^item41$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventIn)
	assert.Equal(snowplowTsv1, eventOut.Data)
	assert.Nil(fail)

	// Wrong event name

	// This configuration says 'keep only `wrong_name`` events whose `sku` field matches "^item41$"'.
	// If the data is not a wrong_name event, the value is nil and it should be filtered out.
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("wrong_name", "", "sku", "^item41$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventIn)
	assert.Equal(snowplowTsv1, eventOut.Data)
	assert.Nil(fail)

	// This configuration says 'keep only `wrong_name`` events whose `ska` field matches "item41"'.
	// If the data the ska field doesn't exist, the value is nil and it should be filtered out.
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "", "ska", "item41", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventNoFieldIn, eventNoFieldOut, fail, _ := eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventNoFieldIn)
	assert.Equal(snowplowTsv1, eventNoFieldOut.Data)
	assert.Nil(fail)

	// This configuration says 'drop `wrong_name`` events whose `sku` field matches "^item41$"'.
	// If the data is not a wrong_name event, the value is nil and it should be kept.
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("wrong_name", "", "sku", "^item41$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventIn.Data)
	assert.Nil(eventOut)
	assert.Nil(fail)

	// This configuration says 'drop `wrong_name`` events whose `ska` field matches "item41"'.
	// If the data the ska field doesn't exist, the value is nil and it should be filtered out.
	eventFilterFunc, err = NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "", "ska", "item41", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventNoFieldIn, eventNoFieldOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(snowplowTsv1, eventNoFieldIn.Data)
	assert.Nil(eventNoFieldOut)
	assert.Nil(fail)
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

	filterFunc, err := NewSpEnrichedFilterFunction("app_id", "^test-data1$", "keep")
	if err != nil {
		fmt.Println(err)
	}

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

	filterFunc2, err := NewSpEnrichedFilterFunction("app_id", "^test-data1|test-data2$", "keep")
	if err != nil {
		fmt.Println(err)
	}

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

	filterFunc3, err := NewSpEnrichedFilterFunction("app_id", "^(test-data1|test-data2)$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	filter3 := NewTransformation(filterFunc3)
	filter3Res := filter3(messages)

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
	aidFilterFuncKeep, err := NewSpEnrichedFilterFunction("app_id", "^test-data3$", "keep")
	if err != nil {
		panic(err)
	}

	aidFilterFuncNegationKeep, _ := NewSpEnrichedFilterFunction("app_id", "^failThis", "drop")
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {

		aidFilterFuncKeep(&messageGood, nil)
		aidFilterFuncNegationKeep(&messageGood, nil)
	}
}

func BenchmarkContextFilter(b *testing.B) {
	var messageGood = models.Message{
		Data:         snowplowTsv3,
		PartitionKey: "some-key",
	}

	contextFuncAffirm, err := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", "keep")
	if err != nil {
		panic(err)
	}
	contextFuncNegate, err := NewSpEnrichedFilterFunctionContext("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^failThis", "drop")
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		contextFuncAffirm(&messageGood, nil)
		contextFuncNegate(&messageGood, nil)
	}
}

func BenchmarkUnstructFilter(b *testing.B) {
	var messageGood = models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}

	unstructFilterFuncAffirm, err := NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^item41$", "keep")
	if err != nil {
		panic(err)
	}
	unstructFilterFuncNegate, err := NewSpEnrichedFilterFunctionUnstructEvent("add_to_cart", "1-*-*", "sku", "^failThis", "keep")
	if err != nil {
		panic(err)
	}

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
