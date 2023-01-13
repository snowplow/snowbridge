//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package filter

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/stretchr/testify/assert"
)

func TestMakeUnstructValueGetter(t *testing.T) {
	assert := assert.New(t)

	re1 := regexp.MustCompile("1-*-*")

	unstructGetter := makeUnstructValueGetter("add_to_cart", re1, []interface{}{"sku"})

	res, err := unstructGetter(transform.SpTsv1Parsed)

	assert.Equal([]interface{}{"item41"}, res)
	assert.Nil(err)

	unstructGetterWrongPath := makeUnstructValueGetter("add_to_cart", re1, []interface{}{"notSku"})

	// If it's not in the event, both should be nil
	res2, err2 := unstructGetterWrongPath(transform.SpTsv1Parsed)

	assert.Nil(res2)
	assert.Nil(err2)

	// test that wrong schema version behaves appropriately (return nil nil)
	re2 := regexp.MustCompile("2-*-*")

	unstructWrongSchemaGetter := makeUnstructValueGetter("add_to_cart", re2, []interface{}{"sku"})

	res3, err3 := unstructWrongSchemaGetter(transform.SpTsv1Parsed)

	assert.Nil(res3)
	assert.Nil(err3)

	// test that not specifying a version behaves appropriately (accepts all versions)
	re3 := regexp.MustCompile("")

	unstructAnyVersionGetter := makeUnstructValueGetter("add_to_cart", re3, []interface{}{"sku"})

	res4, err4 := unstructAnyVersionGetter(transform.SpTsv1Parsed)

	assert.Equal([]interface{}{"item41"}, res4)
	assert.Nil(err4)

	// test that wrong event name behaves appropriately (return nil nil)

	unstructWrongEvnetName := makeUnstructValueGetter("not_add_to_cart_at_all", re3, []interface{}{"sku"})

	res5, err5 := unstructWrongEvnetName(transform.SpTsv1Parsed)

	assert.Nil(res5)
	assert.Nil(err5)
}

func TestNewUnstructFilter(t *testing.T) {
	assert := assert.New(t)

	// event filter success, filtered event name
	eventFilterFunc, err := NewUnstructFilter("add_to_cart", "1-*-*", "sku", "^item41$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ := eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(transform.SnowplowTsv1, eventIn.Data)
	assert.Nil(eventOut)
	assert.Nil(fail)

	// same, with 'drop'
	eventFilterFunc, err = NewUnstructFilter("add_to_cart", "1-*-*", "sku", "^item41$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventIn)
	assert.Equal(transform.SnowplowTsv1, eventOut.Data)
	assert.Nil(fail)

	// event filter success, filtered event name, no event version
	eventFilterFunc, err = NewUnstructFilter("add_to_cart", "", "sku", "^item41$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(transform.SnowplowTsv1, eventIn.Data)
	assert.Nil(eventOut)
	assert.Nil(fail)

	// same with 'drop'
	eventFilterFunc, err = NewUnstructFilter("add_to_cart", "", "sku", "^item41$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventIn)
	assert.Equal(transform.SnowplowTsv1, eventOut.Data)
	assert.Nil(fail)

	// Wrong event name

	// This configuration says 'keep only `wrong_name`` events whose `sku` field matches "^item41$"'.
	// If the data is not a wrong_name event, the value is nil and it should be filtered out.
	eventFilterFunc, err = NewUnstructFilter("wrong_name", "", "sku", "^item41$", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventIn)
	assert.Equal(transform.SnowplowTsv1, eventOut.Data)
	assert.Nil(fail)

	// This configuration says 'keep only `wrong_name`` events whose `ska` field matches "item41"'.
	// If the data the ska field doesn't exist, the value is nil and it should be filtered out.
	eventFilterFunc, err = NewUnstructFilter("add_to_cart", "", "ska", "item41", "keep")
	if err != nil {
		fmt.Println(err)
	}

	eventNoFieldIn, eventNoFieldOut, fail, _ := eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Nil(eventNoFieldIn)
	assert.Equal(transform.SnowplowTsv1, eventNoFieldOut.Data)
	assert.Nil(fail)

	// This configuration says 'drop `wrong_name`` events whose `sku` field matches "^item41$"'.
	// If the data is not a wrong_name event, the value is nil and it should be kept.
	eventFilterFunc, err = NewUnstructFilter("wrong_name", "", "sku", "^item41$", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventIn, eventOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(transform.SnowplowTsv1, eventIn.Data)
	assert.Nil(eventOut)
	assert.Nil(fail)

	// This configuration says 'drop `wrong_name`` events whose `ska` field matches "item41"'.
	// If the data the ska field doesn't exist, the value is nil and it should be filtered out.
	eventFilterFunc, err = NewUnstructFilter("add_to_cart", "", "ska", "item41", "drop")
	if err != nil {
		fmt.Println(err)
	}

	eventNoFieldIn, eventNoFieldOut, fail, _ = eventFilterFunc(&messageWithUnstructEvent, nil)

	assert.Equal(transform.SnowplowTsv1, eventNoFieldIn.Data)
	assert.Nil(eventNoFieldOut)
	assert.Nil(fail)
}

func BenchmarkUnstructFilter(b *testing.B) {
	var messageGood = models.Message{
		Data:         transform.SnowplowTsv1,
		PartitionKey: "some-key",
	}

	unstructFilterFuncAffirm, err := NewUnstructFilter("add_to_cart", "1-*-*", "sku", "^item41$", "keep")
	if err != nil {
		panic(err)
	}
	unstructFilterFuncNegate, err := NewUnstructFilter("add_to_cart", "1-*-*", "sku", "^failThis", "keep")
	if err != nil {
		panic(err)
	}

	for i := 0; i < b.N; i++ {
		unstructFilterFuncAffirm(&messageGood, nil)
		unstructFilterFuncNegate(&messageGood, nil)

	}
}
