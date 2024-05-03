/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package filter

import (
	"fmt"
	"testing"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/stretchr/testify/assert"
)

func TestMakeContextValueGetter(t *testing.T) {
	assert := assert.New(t)

	contextGetter := makeContextValueGetter("contexts_nl_basjes_yauaa_context_1", []interface{}{"test1", "test2", 0, "test3"})

	res, err := contextGetter(transform.SpTsv3Parsed)

	assert.Equal([]interface{}{"testValue"}, res)
	assert.Nil(err)

	res2, err2 := contextGetter(transform.SpTsv1Parsed)

	// If the path doesn't exist, we shoud return nil, nil.
	assert.Nil(res2)
	assert.Nil(err2)

	contextGetterArray := makeContextValueGetter("contexts_com_acme_just_ints_1", []interface{}{"integerField"})

	res3, err3 := contextGetterArray(transform.SpTsv1Parsed)

	assert.Equal([]interface{}{float64(0), float64(1), float64(2)}, res3)
	assert.Nil(err3)
}

func TestNewContextFilter(t *testing.T) {
	assert := assert.New(t)

	// The relevant data in messageGood looks like this: "test1":{"test2":[{"test3":"testValue"}]

	// context filter success
	contextFilterFunc, err := NewContextFilter("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ := contextFilterFunc(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, contextIn.Data)
	assert.Nil(contextOut)
	assert.Nil(fail)

	// same, with 'drop'
	contextFilterFunc, err = NewContextFilter("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Nil(contextIn)
	assert.Equal(transform.SnowplowTsv3, contextOut.Data)
	assert.Nil(fail)

	// The relevant data in messageGoodInt looks like this: "test1":{"test2":[{"test3":1}]

	// context filter success (integer value)
	contextFilterFunc, err = NewContextFilter("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^1$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGoodInt, nil)

	assert.Equal(transform.SnowplowTsv4, contextIn.Data)
	assert.Nil(contextOut)
	assert.Nil(fail)

	// same, with 'drop'
	contextFilterFunc, err = NewContextFilter("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^1$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGoodInt, nil)

	assert.Nil(contextIn)
	assert.Equal(transform.SnowplowTsv4, contextOut.Data)
	assert.Nil(fail)

	// context filter wrong context name
	contextFilterFunc, err = NewContextFilter("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].test3", "^testValue$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Nil(contextIn)
	assert.Equal(transform.SnowplowTsv3, contextOut.Data)
	assert.Nil(fail)

	// Context filter path doesn't exist

	// This configuration is 'keep values that match "^testValue$"'. If the path is wrong, tha value is empty, which doesn't match that regex - so it should be filtered out.
	contextFilterFunc, err = NewContextFilter("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].nothingHere", "^testValue$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Nil(contextIn)
	assert.Equal(transform.SnowplowTsv3, contextOut.Data)
	assert.Nil(fail)

	// This says 'drop values that match "^testValue$"'. If the path is wrong, the value is empty, which doesn't match that regex - so it should be kept.
	contextFilterFunc, err = NewContextFilter("contexts_nl_basjes_yauaa_context_2", "test1.test2[0].nothingHere", "^testValue$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	contextIn, contextOut, fail, _ = contextFilterFunc(&messageGood, nil)

	assert.Equal(transform.SnowplowTsv3, contextIn.Data)
	assert.Nil(contextOut)
	assert.Nil(fail)
}

func BenchmarkContextFilter(b *testing.B) {
	var messageGood = models.Message{
		Data:         transform.SnowplowTsv3,
		PartitionKey: "some-key",
	}

	contextFuncAffirm, err := NewContextFilter("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^testValue$", "keep")
	if err != nil {
		panic(err)
	}
	contextFuncNegate, err := NewContextFilter("contexts_nl_basjes_yauaa_context_1", "test1.test2[0].test3", "^failThis", "drop")
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		contextFuncAffirm(&messageGood, nil)
		contextFuncNegate(&messageGood, nil)
	}
}
