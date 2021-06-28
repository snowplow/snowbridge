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

	// Single value cases
	aidFilterFuncKeep, _ := NewSpEnrichedFilterFunction("app_id==test-data3")

	aidFilteredIn, fail, _ := aidFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredIn.Data)
	assert.Nil(fail)

	aidFilterFuncDiscard, _ := NewSpEnrichedFilterFunction("app_id==failThis")

	aidFilteredOut, fail2, _ := aidFilterFuncDiscard(&messageGood, nil)

	assert.Nil(aidFilteredOut)
	assert.Nil(fail2)

	// int value
	urlPrtFilterFuncKeep, _ := NewSpEnrichedFilterFunction("page_urlport==80")

	urlPrtFilteredIn, fail, _ := urlPrtFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, urlPrtFilteredIn.Data)
	assert.Nil(fail)

	// Multiple value cases
	aidFilterFuncKeepWithMultiple, _ := NewSpEnrichedFilterFunction("app_id==someotherValue|test-data3")

	aidFilteredKeptWithMultiple, fail3, _ := aidFilterFuncKeepWithMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredKeptWithMultiple.Data)
	assert.Nil(fail3)

	aidFilterFuncDiscardWithMultiple, _ := NewSpEnrichedFilterFunction("app_id==someotherValue|failThis")

	aidFilteredDiscardedWithMultiple, fail3, _ := aidFilterFuncDiscardWithMultiple(&messageGood, nil)

	assert.Nil(aidFilteredDiscardedWithMultiple)
	assert.Nil(fail3)

	// Single value negation cases

	aidFilterFuncNegationDiscard, _ := NewSpEnrichedFilterFunction("app_id!=test-data3")

	aidFilteredOutNegated, fail4, _ := aidFilterFuncNegationDiscard(&messageGood, nil)

	assert.Nil(aidFilteredOutNegated)
	assert.Nil(fail4)

	aidFilterFuncNegationKeep, _ := NewSpEnrichedFilterFunction("app_id!=failThis")

	aidFilteredInNegated, fail5, _ := aidFilterFuncNegationKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredInNegated.Data)
	assert.Nil(fail5)

	// Multiple value negation cases
	aidFilterFuncNegationDiscardMultiple, _ := NewSpEnrichedFilterFunction("app_id!=someotherValue|test-data1|test-data2|test-data3")

	aidFilteredDiscardedWithMultiple, fail6, _ := aidFilterFuncNegationDiscardMultiple(&messageGood, nil)

	assert.Nil(aidFilteredDiscardedWithMultiple)
	assert.Nil(fail6)

	aidFilterFuncNegationKeptMultiple, _ := NewSpEnrichedFilterFunction("app_id!=someotherValue|failThis")

	aidFilteredKeptWithMultiple, fail7, _ := aidFilterFuncNegationKeptMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredKeptWithMultiple.Data)
	assert.Nil(fail7)
}

func TestNewSpEnrichedFilterFunctio_Error(t *testing.T) {
	assert := assert.New(t)
	error := `Filter Function Config does not match regex \S+(!=|==)[^\s\|]+((?:\|[^\s|]+)*)$`

	filterFunc, err1 := NewSpEnrichedFilterFunction("")

	assert.Nil(filterFunc)
	assert.Equal(error, err1.Error())

	filterFunc, err2 := NewSpEnrichedFilterFunction("app_id==abc|")

	assert.Nil(filterFunc)
	assert.Equal(error, err2.Error())

	filterFunc, err3 := NewSpEnrichedFilterFunction("!=abc")

	assert.Nil(filterFunc)
	assert.Equal(error, err3.Error())
}

func TestSpEnrichedFilterFunction_Slice(t *testing.T) {
	assert := assert.New(t)

	var expectedFilter1 = []*models.Message{
		{
			Data:         snowplowTsv1,
			PartitionKey: "some-key",
		},
	}

	filterFunc, _ := NewSpEnrichedFilterFunction("app_id==test-data1")

	filter1 := NewTransformation(filterFunc)
	filter1Res := filter1(messages)

	assert.Equal(len(expectedFilter1), len(filter1Res.Result))
	assert.Equal(1, len(filter1Res.Invalid))

	var expectedFilter2 = []*models.Message{
		{
			Data:         snowplowTsv1,
			PartitionKey: "some-key",
		},
		{
			Data:         snowplowTsv2,
			PartitionKey: "some-key1",
		},
	}

	filterFunc2, _ := NewSpEnrichedFilterFunction("app_id==test-data1|test-data2")

	filter2 := NewTransformation(filterFunc2)
	filter2Res := filter2(messages)

	assert.Equal(len(expectedFilter2), len(filter2Res.Result))
	assert.Equal(1, len(filter2Res.Invalid))

	var expectedFilter3 = []*models.Message{
		{
			Data:         snowplowTsv3,
			PartitionKey: "some-key3",
		},
	}

	filterFunc3, _ := NewSpEnrichedFilterFunction("app_id!=test-data1|test-data2")

	filter3 := NewTransformation(filterFunc3)
	filter3Res := filter3(messages)

	assert.Equal(len(expectedFilter3), len(filter3Res.Result))
	assert.Equal(1, len(filter3Res.Invalid))

}
