// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package filter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

func TestMakeBaseValueGetter(t *testing.T) {
	assert := assert.New(t)

	// simple app ID
	appIDGetter := makeBaseValueGetter("app_id")

	res, err := appIDGetter(transform.SpTsv3Parsed)

	assert.Equal([]interface{}{"test-data3"}, res)
	assert.Nil(err)

	// Leaving the test here as it accurately describes the getter's behaviour,
	// but we now should never hit this in practice, since we valdiate in NewAtomicFilter.
	nonExistentFieldGetter := makeBaseValueGetter("nope")

	res2, err2 := nonExistentFieldGetter(transform.SpTsv3Parsed)

	assert.Nil(res2)
	assert.NotNil(err2)
	if err2 != nil {
		assert.Equal("Key nope not a valid atomic field", err2.Error())
	}
}

func TestNewAtomicFilter(t *testing.T) {
	assert := assert.New(t)

	// Single value cases
	aidFilterFuncKeep, err := NewAtomicFilterFunction("app_id", "^test-data3$", "keep")
	if err != nil {
		panic(err)
	}

	aidKeepIn, aidKeepOut, fail, _ := aidFilterFuncKeep(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, aidKeepIn.Data)
	assert.Nil(aidKeepOut)
	assert.Nil(fail)

	aidFilterFuncDiscard, err := NewAtomicFilterFunction("app_id", "failThis", "keep")
	if err != nil {
		panic(err)
	}

	aidDiscardIn, aidDiscardOut, fail2, _ := aidFilterFuncDiscard(&messageGood, nil)

	assert.Nil(aidDiscardIn)
	assert.Equal(transform.SnowplowTsv3, aidDiscardOut.Data)
	assert.Nil(fail2)

	// int value
	urlPrtFilterFuncKeep, err := NewAtomicFilterFunction("page_urlport", "^80$", "keep")
	if err != nil {
		panic(err)
	}

	urlPrtKeepIn, urlPrtKeepOut, fail, _ := urlPrtFilterFuncKeep(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, urlPrtKeepIn.Data)
	assert.Nil(urlPrtKeepOut)
	assert.Nil(fail)

	// Multiple value cases
	aidFilterFuncKeepWithMultiple, err := NewAtomicFilterFunction("app_id", "^someotherValue|test-data3$", "keep")
	if err != nil {
		panic(err)
	}

	aidMultipleIn, aidMultipleOut, fail, _ := aidFilterFuncKeepWithMultiple(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, aidMultipleIn.Data)
	assert.Nil(aidMultipleOut)
	assert.Nil(fail)

	aidFilterFuncDiscardWithMultiple, _ := NewAtomicFilterFunction("app_id", "^someotherValue|failThis$", "keep")
	if err != nil {
		panic(err)
	}

	aidNoneOfMultipleIn, aidNoneOfMultipleOut, fail, _ := aidFilterFuncDiscardWithMultiple(&messageGood, nil)

	assert.Nil(aidNoneOfMultipleIn)
	assert.Equal(transform.SnowplowTsv3, aidNoneOfMultipleOut.Data)
	assert.Nil(fail)

	// Single value negation cases
	aidFilterFuncNegationDiscard, err := NewAtomicFilterFunction("app_id", "^test-data3", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidNegationIn, aidNegationOut, fail, _ := aidFilterFuncNegationDiscard(&messageGood, nil)

	assert.Nil(aidNegationIn)
	assert.Equal(transform.SnowplowTsv3, aidNegationOut.Data)
	assert.Nil(fail)

	aidFilterFuncNegationKeep, err := NewAtomicFilterFunction("app_id", "^someValue", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidNegationFailedIn, aidNegationFailedOut, fail, _ := aidFilterFuncNegationKeep(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, aidNegationFailedIn.Data)
	assert.Nil(aidNegationFailedOut)
	assert.Nil(fail)

	// Multiple value negation cases
	aidFilterFuncNegationDiscardMultiple, err := NewAtomicFilterFunction("app_id", "^(someotherValue|test-data1|test-data2|test-data3)", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidNoneOfMultipleIn, aidNegationMultipleOut, fail, _ := aidFilterFuncNegationDiscardMultiple(&messageGood, nil)

	assert.Nil(aidNoneOfMultipleIn)
	assert.Equal(transform.SnowplowTsv3, aidNegationMultipleOut.Data)
	assert.Nil(fail)

	aidFilterFuncNegationKeptMultiple, err := NewAtomicFilterFunction("app_id", "^(someotherValue|failThis)$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	aidMultipleIn, aidMultipleNegationFailedOut, fail, _ := aidFilterFuncNegationKeptMultiple(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, aidMultipleIn.Data)
	assert.Nil(aidMultipleNegationFailedOut)
	assert.Nil(fail)

	// Filters on a nil field
	txnFilterFunctionAffirmation, err := NewAtomicFilterFunction("txn_id", "^something$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	nilAffirmationIn, nilAffirmationOut, fail, _ := txnFilterFunctionAffirmation(&messageGood, nil)

	// nil doesn't match the regex and should be filtered out.
	assert.Nil(nilAffirmationIn)
	assert.Equal(transform.SnowplowTsv3, nilAffirmationOut.Data)
	assert.Nil(fail)

	txnFilterFunctionNegation, err := NewAtomicFilterFunction("txn_id", "^something", "drop")
	if err != nil {
		fmt.Println(err)
	}

	nilNegationIn, nilNegationOut, fail, _ := txnFilterFunctionNegation(&messageGood, nil)

	// nil DOES match the negative lookup - it doesn't contain 'something'. So should be kept.
	assert.Equal(transform.SnowplowTsv3, nilNegationIn.Data)
	assert.Nil(nilNegationOut)
	assert.Nil(fail)

	// if the field provided isn't a valid atomic field, we should fail on startup.
	fieldNotExistsFilter, err := NewAtomicFilterFunction("nothing", "", "keep")

	assert.Nil(fieldNotExistsFilter)
	assert.NotNil(err)
}

func BenchmarkAtomicFilter(b *testing.B) {
	var messageGood = models.Message{
		Data:         transform.SnowplowTsv3,
		PartitionKey: "some-key",
	}
	aidFilterFuncKeep, err := NewAtomicFilterFunction("app_id", "^test-data3$", "keep")
	if err != nil {
		panic(err)
	}

	aidFilterFuncNegationKeep, _ := NewAtomicFilterFunction("app_id", "^failThis", "drop")
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {

		aidFilterFuncKeep(&messageGood, nil)
		aidFilterFuncNegationKeep(&messageGood, nil)
	}
}
