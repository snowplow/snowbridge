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
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
)

func TestJQRunFunction_SpMode_true(t *testing.T) {
	testCases := []struct {
		Scenario        string
		JQCommand       string
		InputMsg        *models.Message
		InputInterState interface{}
		Expected        map[string]*models.Message
		ExpInterState   interface{}
		Error           error
	}{
		{
			Scenario:  "happy_path",
			JQCommand: `{foo: .app_id}`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"test-data1"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jqConfig := &JQMapperConfig{
				JQCommand:  tt.JQCommand,
				RunTimeout: 15,
				SpMode:     true,
			}

			transFun, err := JQMapperConfigFunction(jqConfig)
			assert.Nil(err)

			s, f, e, i := transFun(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareJQ(t, s, tt.Expected["success"], "success")
			assertMessagesCompareJQ(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareJQ(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func TestJQRunFunction_SpMode_false(t *testing.T) {
	testCases := []struct {
		Scenario        string
		JQCommand       string
		InputMsg        *models.Message
		InputInterState interface{}
		Expected        map[string]*models.Message
		ExpInterState   interface{}
		Error           error
	}{
		{
			Scenario:  "happy_path",
			JQCommand: `{foo: .app_id}`,
			InputMsg: &models.Message{
				Data:         snowplowJSON1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"test-data1"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario: "with_multiple_returns",
			JQCommand: `
{
    bar: .foo | ..
}`,
			InputMsg: &models.Message{
				Data:         []byte(`{"foo":[1,2,3]}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"bar":[1,2,3]}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jqConfig := &JQMapperConfig{
				JQCommand:  tt.JQCommand,
				RunTimeout: 15,
				SpMode:     false,
			}

			transFun, err := JQMapperConfigFunction(jqConfig)
			assert.Nil(err)

			s, f, e, i := transFun(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareJQ(t, s, tt.Expected["success"], "success")
			assertMessagesCompareJQ(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareJQ(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func TestJQRunFunction_errors(t *testing.T) {
	testCases := []struct {
		Scenario        string
		JQConfig        *JQMapperConfig
		InputMsg        *models.Message
		InputInterState interface{}
		Expected        map[string]*models.Message
		ExpInterState   interface{}
		Error           error
	}{
		{
			Scenario: "happy_path",
			JQConfig: &JQMapperConfig{
				JQCommand:  `{foo: .app_id}`,
				RunTimeout: 0,
				SpMode:     true,
			},
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         SnowplowTsv1,
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("context deadline"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			transFun, err := JQMapperConfigFunction(tt.JQConfig)
			assert.Nil(err)

			s, f, e, i := transFun(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareJQ(t, s, tt.Expected["success"], "success")
			assertMessagesCompareJQ(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareJQ(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func assertMessagesCompareJQ(t *testing.T, act, exp *models.Message, hint string) {
	t.Helper()
	ok := false
	headersOk := false
	switch {
	case act == nil:
		ok = exp == nil
	case exp == nil:
	default:
		pkOk := act.PartitionKey == exp.PartitionKey
		dataOk := reflect.DeepEqual(act.Data, exp.Data)
		cTimeOk := reflect.DeepEqual(act.TimeCreated, exp.TimeCreated)
		pTimeOk := reflect.DeepEqual(act.TimePulled, exp.TimePulled)
		tTimeOk := reflect.DeepEqual(act.TimeTransformed, exp.TimeTransformed)
		ackOk := reflect.DeepEqual(act.AckFunc, exp.AckFunc)
		headersOk = reflect.DeepEqual(act.HTTPHeaders, exp.HTTPHeaders)

		if pkOk && dataOk && cTimeOk && pTimeOk && tTimeOk && ackOk && headersOk {
			ok = true
		}
	}

	if !ok {
		// message.HTTPHeaders are not printed
		if headersOk == false && act != nil && exp != nil {
			t.Errorf("\nUnexpected HTTPHeaders:\nGOT:\n%s\nEXPECTED:\n%s\n",
				spew.Sdump(act.HTTPHeaders),
				spew.Sdump(exp.HTTPHeaders))
		} else {
			t.Errorf("\nGOT:\n%s\nEXPECTED:\n%s\n",
				spew.Sdump(act),
				spew.Sdump(exp))
		}
	}
}

// tmp -  Commenting out for reference

// func TestJustJQ(t *testing.T) {
// 	// assert := assert.New(t)

// 	inputData := &models.Message{
// 		Data:         snowplowJSON1,
// 		PartitionKey: "some-key",
// 	}

// 	var input map[string]any

// 	json.Unmarshal(inputData.Data, &input)

// 	res := grabFromGenericJQConfig(input, examplePureJQConfig)

// 	fmt.Println(string(res))
// }

// func TestGrabValue(t *testing.T) {
// 	assert := assert.New(t)

// 	inputData := &models.Message{
// 		Data:         snowplowJSON1,
// 		PartitionKey: "some-key",
// 	}

// 	query, err := gojq.Parse(".contexts_com_acme_just_ints_1[0].integerField")
// 	if err != nil {
// 		panic(err)
// 	}

// 	var input map[string]any

// 	json.Unmarshal(inputData.Data, &input)

// 	valueFound, err := grabValue(input, query)
// 	if err != nil {
// 		panic(err)
// 	}

// 	assert.Equal(float64(0), valueFound)

// }

// func TestMapper(t *testing.T) {
// 	assert := assert.New(t)

// 	// Mapper(&models.Message{
// 	// 	Data:         snowplowJSON1,
// 	// 	PartitionKey: "some-key",
// 	// }, nil)

// 	inputData := &models.Message{
// 		Data:         snowplowJSON1,
// 		PartitionKey: "some-key",
// 	}

// 	assert.Nil(nil)

// 	var input map[string]any

// 	json.Unmarshal(inputData.Data, &input)

// 	mapped := grabLotsOfValues(input, exampleParsedConfig)

// 	fmt.Println(mapped)

// 	// expectedMap := map[string]any{
// 	// 	"arraySpecified": []string{"test-data1", "e9234345-f042-46ad-b1aa-424464066a33"},
// 	// 	"field1":         "test-data1",
// 	// 	"field2": map[string]any{
// 	// 		"nestedField1": map[string]any{
// 	// 			"integerField": float64(0),
// 	// 		},
// 	// 	},
// 	// 	// "fieldWithCoalesceExample":
// 	// }

// 	// assert.Equal(expectedMap, mapped)
// }

// /*
//  fieldWithCoalesceExample:map[coalesce:[map[integerField:0]]] fieldWithOtherCoalesceExample:test-data1 manualUnnest:map[just_ints_integerField:0]]
// */
