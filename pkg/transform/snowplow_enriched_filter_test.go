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

	// Single value affirmation cases
	aidFilterFuncKeep := NewSpEnrichedFilterFunction("app_id==test-data3")

	aidFilteredIn, fail, _ := aidFilterFuncKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredIn.Data)
	assert.Nil(fail)

	aidFilterFuncDiscard := NewSpEnrichedFilterFunction("app_id==failThis")

	aidFilteredOut, fail2, _ := aidFilterFuncDiscard(&messageGood, nil)

	assert.Nil(aidFilteredOut)
	assert.Nil(fail2)

	// Multiple value cases
	aidFilterFuncKeepWithMultiple := NewSpEnrichedFilterFunction("app_id==someotherValue|test-data3")

	aidFilteredKeptWithMultiple, fail3, _ := aidFilterFuncKeepWithMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredKeptWithMultiple.Data)
	assert.Nil(fail3)

	aidFilterFuncDiscardWithMultiple := NewSpEnrichedFilterFunction("app_id==someotherValue|failThis")

	aidFilteredDiscardedWithMultiple, fail3, _ := aidFilterFuncDiscardWithMultiple(&messageGood, nil)

	assert.Nil(aidFilteredDiscardedWithMultiple)
	assert.Nil(fail3)

	// Single value negation cases

	aidFilterFuncNegationDiscard := NewSpEnrichedFilterFunction("app_id!=test-data3")

	aidFilteredOutNegated, fail4, _ := aidFilterFuncNegationDiscard(&messageGood, nil)

	assert.Nil(aidFilteredOutNegated)
	assert.Nil(fail4)

	aidFilterFuncNegationKeep := NewSpEnrichedFilterFunction("app_id!=failThis")

	aidFilteredInNegated, fail5, _ := aidFilterFuncNegationKeep(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredInNegated.Data)
	assert.Nil(fail5)

	// Multiple value negation cases
	aidFilterFuncNegationDiscardMultiple := NewSpEnrichedFilterFunction("app_id!=someotherValue|test-data1|test-data2|test-data3")

	aidFilteredDiscardedWithMultiple, fail6, _ := aidFilterFuncNegationDiscardMultiple(&messageGood, nil)

	assert.Nil(aidFilteredDiscardedWithMultiple)
	assert.Nil(fail6)

	aidFilterFuncNegationKeptMultiple := NewSpEnrichedFilterFunction("app_id!=someotherValue|failThis")

	aidFilteredKeptWithMultiple, fail7, _ := aidFilterFuncNegationKeptMultiple(&messageGood, nil)

	assert.Equal(snowplowTsv3, aidFilteredKeptWithMultiple.Data)
	assert.Nil(fail7)
}

func TestSpEnrichedFilterFunction_Slice(t *testing.T) {
	assert := assert.New(t)

	var expectedFilter1 = []*models.Message{
		{
			Data:         snowplowTsv1,
			PartitionKey: "some-key",
		},
	}

	filter1 := NewTransformation(NewSpEnrichedFilterFunction("app_id==test-data1"))
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

	filter2 := NewTransformation(NewSpEnrichedFilterFunction("app_id==test-data1|test-data2"))
	filter2Res := filter2(messages)

	assert.Equal(len(expectedFilter2), len(filter2Res.Result))
	assert.Equal(1, len(filter2Res.Invalid))

	var expectedFilter3 = []*models.Message{
		{
			Data:         snowplowTsv3,
			PartitionKey: "some-key3",
		},
	}

	filter3 := NewTransformation(NewSpEnrichedFilterFunction("app_id!=test-data1|test-data2"))
	filter3Res := filter3(messages)

	assert.Equal(len(expectedFilter3), len(filter3Res.Result))
	assert.Equal(1, len(filter3Res.Invalid))

	/*
		for index, value := range enrichJsonRes.Result {
			assert.Equal(expectedGood[index].Data, value.Data)
			assert.Equal(expectedGood[index].PartitionKey, value.PartitionKey)
			assert.NotNil(expectedGood[index].TimeTransformed)

			// assertions to ensure we don't accidentally modify the input
			assert.NotEqual(messages[index].Data, value.Data)
			// assert can't seem to deal with comparing zero value to non-zero value, so assert that it's still zero instead
			assert.Equal(time.Time{}, messages[index].TimeTransformed)
		}

		// Not matching equivalence of whole object because error stacktrace makes it unfeasible. Doing each component part instead.
		assert.Equal(1, len(enrichJsonRes.Invalid))
		assert.Equal(int64(1), enrichJsonRes.InvalidCount)
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", enrichJsonRes.Invalid[0].GetError().Error())
		assert.Equal([]byte("not	a	snowplow	event"), enrichJsonRes.Invalid[0].Data)
		assert.Equal("some-key4", enrichJsonRes.Invalid[0].PartitionKey)
	*/
}

// TODO: add tests checking slice of messages against output.
