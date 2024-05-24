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

package transform

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/itchyny/gojq"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestGrabValue(t *testing.T) {
	assert := assert.New(t)

	inputData := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-key",
	}

	query, err := gojq.Parse(".contexts_com_acme_just_ints_1[0].integerField")
	if err != nil {
		panic(err)
	}

	var input map[string]any

	json.Unmarshal(inputData.Data, &input)

	valueFound, err := grabValue(input, query)
	if err != nil {
		panic(err)
	}

	assert.Equal(float64(0), valueFound)

}

func TestMapper(t *testing.T) {
	assert := assert.New(t)

	// Mapper(&models.Message{
	// 	Data:         snowplowJSON1,
	// 	PartitionKey: "some-key",
	// }, nil)

	inputData := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-key",
	}

	assert.Nil(nil)

	var input map[string]any

	json.Unmarshal(inputData.Data, &input)

	mapped := grabLotsOfValues(input, exampleParsedConfig)

	fmt.Println(mapped)

	// expectedMap := map[string]any{
	// 	"arraySpecified": []string{"test-data1", "e9234345-f042-46ad-b1aa-424464066a33"},
	// 	"field1":         "test-data1",
	// 	"field2": map[string]any{
	// 		"nestedField1": map[string]any{
	// 			"integerField": float64(0),
	// 		},
	// 	},
	// 	// "fieldWithCoalesceExample":
	// }

	// assert.Equal(expectedMap, mapped)
}

/*
 fieldWithCoalesceExample:map[coalesce:[map[integerField:0]]] fieldWithOtherCoalesceExample:test-data1 manualUnnest:map[just_ints_integerField:0]]
*/
