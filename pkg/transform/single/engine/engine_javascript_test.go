// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
package engine

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
)

type JSTestCase struct {
	Scenario      string
	Src           string
	SpMode        bool
	Input         *models.Message
	InterState    interface{}
	Expected      map[string]*models.Message
	ExpInterState interface{}
	IsJSON        bool
	Error         error
}

func TestJSLayer(t *testing.T) {
	assert := assert.New(t)

	script := `
	function foo(x) {
	    return x;
	}`

	jsEngine, err := NewJSEngine(&JSEngineConfig{
		RunTimeout: 15,
		SpMode:     false,
	}, script)
	assert.NotNil(t, jsEngine)
	assert.Nil(err)
}

func TestJSEngineMakeFunction_SpModeFalse_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode = false
	testCases := []JSTestCase{
		{
			Src: `
function main(x) {
   return x;
}
`,
			Scenario: "identity",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         "asdf",
			},
			Error: nil,
		},
		{
			Src: `
function main(x) {
   let newVal = "Hello:" + x.Data;
   x.Data = newVal;
   return x;
}
`,
			Scenario: "concatHello",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("Hello:asdf"),
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         "Hello:asdf",
			},
			Error: nil,
		},
		{
			Src: `
function main(x) {
   x.FilterOut = false
   return x;
}
`,
			Scenario: "filterIn",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         "asdf",
			},
			Error: nil,
		},
		{
			Src: `
function main(x) {
   if (Object.prototype.toString.call(x.Data) === '[object String]') {
       return {
           FilterOut: true,
       };
   }

   return {
       FilterOut: false,
       Data: x.Data
   };
}
`,
			Scenario: "filterOut",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Src: `
function main(x) {
   var jsonObj = JSON.parse(x.Data);
   var result = JSON.stringify(jsonObj);

   return {
       Data: result
   };
}
`,
			Scenario: "jsonIdentity",
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Src: `
function main(x) {
   var jsonObj = JSON.parse(x.Data);

   if (jsonObj.hasOwnProperty("app_id")) {
       x.Data = x.Data.replace(/app_id/, 'app_id_CHANGED');
   }

   return x;
}
`,
			Scenario: "jsonTransformFieldNameRegex",
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSONChanged1,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSONChanged1),
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Src: `
function main(x) {

   var jsonObj = JSON.parse(x.Data);

   var descriptor = Object.getOwnPropertyDescriptor(jsonObj, "app_id");
   Object.defineProperty(jsonObj, "app_id_CHANGED", descriptor);
   delete jsonObj["app_id"];

   return {
       Data: JSON.stringify(jsonObj)
   };
}
`,
			Scenario: "jsonTransformFieldNameObj",
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSONChanged2,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSONChanged2),
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Src: `
function main(x) {
   var jsonObj = JSON.parse(x.Data);

   if (jsonObj.hasOwnProperty("app_id") && jsonObj["app_id"] === "filterMeOut") {
       x.FilterOut = false;
   } else {
       x.FilterOut = true;
   }

   return x;
}
`,
			Scenario: "jsonFilterOut",
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			IsJSON:        true,
			Error:         nil,
		},
		{
			Src: `
function main(x) {
   return 0;
}
`,
			Scenario: "returnWrongType",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("invalid return type from JavaScript transformation"),
		},
		{
			Src: `
function main(x) {}
`,
			Scenario: "returnUndefined",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("invalid return type from JavaScript transformation; got null or undefined"),
		},
		{
			Src: `
function main(x) {
 return null;
}
`,
			Scenario: "returnNull",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("invalid return type from JavaScript transformation; got null or undefined"),
		},
		{
			Src: `
function main(x) {
   return x.toExponential(2);
}
`,
			Scenario: "causeRuntimeError",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf(`error running JavaScript function "main": "TypeError: Object has no member 'toExponential' at main`),
		},
		{
			Src: `
function main(x) {
   throw("Failed");
}
`,
			Scenario: "callError",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf(`error running JavaScript function "main": "Failed at main`),
		},
		{
			Src: `
function main(x) {
   var now = new Date().getTime();
   while(new Date().getTime() < now + 10000) {
   }
}
`,
			Scenario: "sleepTenSecs",
			Input: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("runtime deadline exceeded"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
				SpMode:     testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig, tt.Src)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest("main"); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction("main")
			s, f, e, i := transFunction(tt.Input, testInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], tt.IsJSON)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], tt.IsJSON)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], tt.IsJSON)
		})
	}
}

func TestJSEngineMakeFunction_SpModeTrue_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = true
	testCases := []JSTestCase{
		{
			Scenario: "identity",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "filtering",
			Src: `
function main(input) {
   // input is an object
   var spData = input.Data;
   if (spData["app_id"] === "myApp") {
       return input;
   }
   return {
       FilterOut: true
   };
}
`,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testJsTsv,
					PartitionKey: "some-test-key",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario: "filteringOut_ignoresData",
			Src: `
function main(x) {
   return {
       FilterOut: true,
       Data: "shouldNotAppear",
       PartitionKey: "notThis"
   };
}
`,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testJsTsv,
					PartitionKey: "some-test-key",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario: "non_Snowplow_enriched_to_failed",
			Src: `
function main(x) {
  return x;
}
`,
			Input: &models.Message{
				Data:         []byte("nonSpEnrichedEvent"),
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("nonSpEnrichedEvent"),
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("Cannot parse"),
		},
		{
			Scenario: "return_wrong_type",
			Src: `
function main(x) {
   return 0;
}
`,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         testJsTsv,
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("invalid return type from JavaScript transformation"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
				SpMode:     testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig, tt.Src)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest("main"); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction("main")
			s, f, e, i := transFunction(tt.Input, testInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], tt.IsJSON)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], tt.IsJSON)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], tt.IsJSON)
		})
	}
}

func TestJSEngineMakeFunction_IntermediateState_SpModeFalse(t *testing.T) {
	testSpMode := false
	testCases := []JSTestCase{
		{
			Scenario: "intermediateState_EngineProtocol_Map",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_spMode_true",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_spMode_false",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			IsJSON: true,
			Error:  nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
				SpMode:     testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig, tt.Src)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest("main"); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction("main")
			s, f, e, i := transFunction(tt.Input, tt.InterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], tt.IsJSON)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], tt.IsJSON)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], tt.IsJSON)
		})
	}
}

func TestJSEngineMakeFunction_IntermediateState_SpModeTrue(t *testing.T) {
	testSpMode := true
	testCases := []JSTestCase{
		{
			Scenario: "intermediateState_EngineProtocol_Map",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "intermediateState_notEngineProtocol_notSpEnriched",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			IsJSON:        true,
			Error:         fmt.Errorf("Cannot parse"),
		},
		{
			Scenario: "intermediateState_notEngineProtocol_SpEnriched",
			Src: `
function main(x) {
   return x;
}
`,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			IsJSON: true,
			Error:  nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
				SpMode:     testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig, tt.Src)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest("main"); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction("main")
			s, f, e, i := transFunction(tt.Input, tt.InterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], tt.IsJSON)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], tt.IsJSON)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], tt.IsJSON)
		})
	}
}

func TestJSEngineMakeFunction_SetPK(t *testing.T) {
	var testInterState interface{} = nil
	testCases := []JSTestCase{
		{
			Scenario: "onlySetPk_spModeTrue",
			Src: `
function main(x) {
   x.PartitionKey = "newPk";
   return x;
}
`,
			SpMode: true,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "oldPK",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "newPk",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         testJSMap,
			},
			IsJSON: true,
			Error:  nil,
		},
		{
			Scenario: "onlySetPk_spModeFalse",
			Src: `
function main(x) {
   x.PartitionKey = "newPk";
   return x;
}
`,
			SpMode: false,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "oldPK",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsTsv,
					PartitionKey: "newPk",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         string(testJsTsv),
			},
			Error: nil,
		},
		{
			Scenario: "filterOutIgnores",
			Src: `
function main(x) {
   return {
       FilterOut: true,
       Data: "shouldNotAppear",
       PartitionKey: "notThis"
   };
}
`,
			SpMode: true,
			Input: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "oldPk",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testJsTsv,
					PartitionKey: "oldPk",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
				SpMode:     tt.SpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig, tt.Src)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest("main"); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction("main")
			s, f, e, i := transFunction(tt.Input, testInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], tt.IsJSON)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], tt.IsJSON)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], tt.IsJSON)
		})
	}
}

func TestJSEngineMakeFunction_HTTPHeaders(t *testing.T) {
	testCases := []struct {
		Scenario        string
		Src             string
		SpMode          bool
		InputMsg        *models.Message
		InputInterState interface{}
		Expected        map[string]*models.Message
		IsJSON          bool
		ExpInterState   interface{}
		Error           error
	}{
		{
			Scenario: "without_initial_headers_main_case",
			Src: `
function main(x) {
  var httpHeaders = {
    foo: 'bar'
  };
  x.HTTPHeaders = httpHeaders;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"foo": "bar",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders: map[string]string{
					"foo": "bar",
				},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_main_case_override",
			Src: `
function main(x) {
  x.HTTPHeaders = {
    foo: 'bar'
  };
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"foo": "bar",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders: map[string]string{
					"foo": "bar",
				},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_main_case_append",
			Src: `
function main(x) {
  var headers = x.HTTPHeaders || {};
  headers.foo = 'bar';
  x.HTTPHeaders = headers;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
						"foo":    "bar",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
					"foo":    "bar",
				},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_and_intermediate_main_case_override",
			Src: `
function main(x) {
  var httpHeaders = {
    foo: 'bar'
  };
  x.HTTPHeaders = httpHeaders;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: &engineProtocol{
				Data: testJSMap,
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"foo": "bar",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders: map[string]string{
					"foo": "bar",
				},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_and_intermediate_main_case_append",
			Src: `
function main(x) {
  var headers = x.HTTPHeaders || {};
  var foo = 'bar';
  var old = headers.oldKey || '';

  headers.foo = foo;
  headers.oldKey = old.concat('newVal');
  x.HTTPHeaders = headers;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: &engineProtocol{
				Data: testJSMap,
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldValnewVal",
						"foo":    "bar",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders: map[string]string{
					"oldKey": "oldValnewVal",
					"foo":    "bar",
				},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_set_to_null_no_effect",
			Src: `
function main(x) {
  x.HTTPHeaders = null;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders:  nil,
			},
			Error: nil,
		},
		{
			Scenario: "without_initial_headers_set_to_null_no_effect",
			Src: `
function main(x) {
  x.HTTPHeaders = null;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders:  nil,
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders:  nil,
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_set_to_undefined_no_effect",
			Src: `
function main(x) {
  x.HTTPHeaders = undefined;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders:  nil,
			},
			Error: nil,
		},
		{
			Scenario: "without_initial_headers_set_to_undefined_no_effect",
			Src: `
function main(x) {
  x.HTTPHeaders = undefined;
  return x;
}
`,
			SpMode: true,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSON,
					PartitionKey: "pk",
					HTTPHeaders:  nil,
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: true,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
				HTTPHeaders:  nil,
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_set_to_empty_object_no_effect",
			Src: `
function main(x) {
  var newHeaders = {};
  x.HTTPHeaders = newHeaders;
  return x;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: false,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				Data:         "asdf",
				PartitionKey: "",
				HTTPHeaders:  map[string]string{},
			},
			Error: nil,
		},
		{
			Scenario: "without_initial_headers_set_to_empty_object",
			Src: `
function main(x) {
  var newHeaders = {};
  x.HTTPHeaders = newHeaders;
  return x;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders:  nil,
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: false,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				Data:         "asdf",
				PartitionKey: "",
				HTTPHeaders:  map[string]string{},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_mutate_set_headers_to_invalid_primitive",
			Src: `
function main(x) {
  x.HTTPHeaders = 'invalid';
  return x;
}
`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: &engineProtocol{
				Data: []byte("asdf"),
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
					},
				},
			},
			IsJSON:        false,
			ExpInterState: nil,
			Error:         fmt.Errorf("could not convert"),
		},
		{
			Scenario: "with_initial_headers_replace_set_headers_to_invalid_primitive",
			Src: `
function main(x) {
  return {
    Data: x.Data,
    PrimaryKey: x.PrimaryKey,
    HTTPHeaders: 'invalid'
  };
}
`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: &engineProtocol{
				Data: []byte("asdf"),
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
					},
				},
			},
			IsJSON:        false,
			ExpInterState: nil,
			Error:         fmt.Errorf("protocol violation"),
		},
		{
			Scenario: "with_initial_headers_mutate_set_headers_to_invalid_object_function_as_empty_object",
			Src: `
function main(x) {
  var newHeaders = function(y) {return y;};
  x.HTTPHeaders = newHeaders;
  return x;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"oldKey": "oldVal",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: false,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				Data:         "asdf",
				PartitionKey: "",
				HTTPHeaders:  map[string]string{},
			},
			Error: nil,
		},
		{
			Scenario: "with_initial_headers_mutate_set_headers_to_invalid_object_array_as_object",
			Src: `
function main(x) {
  var newHeaders = [10, 11];
  x.HTTPHeaders = newHeaders;
  return x;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
				HTTPHeaders: map[string]string{
					"oldKey": "oldVal",
				},
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"0": "10",
						"1": "11",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: false,
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				Data:         "asdf",
				PartitionKey: "",
				HTTPHeaders: map[string]string{
					"0": "10",
					"1": "11",
				},
			},
			Error: nil,
		},
		{
			Scenario: "without_initial_headers_set_invalid_header_value(object)_calls_toString",
			Src: `
function main(x) {
  var newHeaders = {invalid: {}};
  x.HTTPHeaders = newHeaders;
  return x;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
					HTTPHeaders: map[string]string{
						"invalid": "[object Object]",
					},
				},
				"filtered": nil,
				"failed":   nil,
			},
			IsJSON: false,
			ExpInterState: &engineProtocol{
				Data:         "asdf",
				PartitionKey: "",
				HTTPHeaders: map[string]string{
					"invalid": "[object Object]",
				},
			},
			Error: nil,
		},
		{
			Scenario: "without_initial_headers_set_to_invalid_object(e.g.array)_replace",
			Src: `
function main(x) {
  var newHeaders = [];
  var ret = {
    FilterOut: x.FilterOut,
    Data: x.Data,
    PartitionKey: x.PartitionKey,
    HTTPHeaders: newHeaders
  };

  return ret;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         []byte("asdf"),
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte("asdf"),
					PartitionKey: "pk",
				},
			},
			IsJSON:        false,
			ExpInterState: nil,
			Error:         fmt.Errorf("protocol violation"),
		},

		{
			Scenario: "filterOut_ignores_headers",
			Src: `
function main(x) {
  x.HTTPHeaders = {
    foo: 'bar'
  };
  x.FilterOut = true;
  return x;
}
		`,
			SpMode: false,
			InputMsg: &models.Message{
				Data:         testJsTsv,
				PartitionKey: "pk",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testJsTsv,
					PartitionKey: "pk",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
				SpMode:     tt.SpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig, tt.Src)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest("main"); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction("main")
			s, f, e, i := transFunction(tt.InputMsg, tt.InputInterState)

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

			assertMessagesCompareJs(t, s, tt.Expected["success"], tt.IsJSON)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], tt.IsJSON)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], tt.IsJSON)
		})
	}
}

func TestJSEngineSmokeTest(t *testing.T) {
	testCases := []struct {
		Src          string
		FunName      string
		CompileError error
		SmokeError   error
	}{
		{
			Src: `
function identity(x) {
   return x;
}
`,
			FunName:      "identity",
			CompileError: nil,
			SmokeError:   nil,
		},
		{
			Src: `
function notMain(x) {
   return x;
}
`,
			FunName:      "notExists",
			CompileError: nil,
			SmokeError:   fmt.Errorf("could not assert as function"),
		},
		{
			Src: `
function main(x) {
   local y = 0;
}
`,
			FunName:      "syntaxError",
			CompileError: fmt.Errorf("SyntaxError"),
			SmokeError:   nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.FunName, func(t *testing.T) {
			assert := assert.New(t)

			jsConfig := &JSEngineConfig{
				RunTimeout: 5,
			}

			jsEngine, compileErr := NewJSEngine(jsConfig, tt.Src)

			if compileErr != nil {
				if tt.CompileError == nil {
					t.Fatalf("got unexpected error while creating NewJSEngine: %s", compileErr.Error())
				}

				if !strings.Contains(compileErr.Error(), tt.CompileError.Error()) {
					t.Errorf("NewJSEngine error mismatch\nGOT_ERROR:\n%q\n does not contain\nEXPECTED_ERROR:\n%q",
						compileErr.Error(),
						tt.CompileError.Error())
				}
			} else {
				assert.NotNil(jsEngine)

				smoke := jsEngine.SmokeTest(tt.FunName)
				expErr := tt.SmokeError
				if smoke != nil {
					if expErr == nil {
						t.Fatalf("got unexpected smoke-test error: %q", smoke.Error())
					}

					if !strings.Contains(smoke.Error(), expErr.Error()) {
						t.Errorf("smoke error mismatch\nGOT_ERROR:\n%q\ndoes not contain\nEXPECTED_ERROR:\n%q",
							smoke.Error(),
							expErr.Error())
					}
				} else {
					assert.Nil(tt.SmokeError)
				}
			}
		})
	}
}

func Benchmark_JSEngine_Passthrough_DisabledSrcMaps(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x) {
   return x;
}
`
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		RunTimeout: 5,
	}

	jsEngine, err := NewJSEngine(jsConfig, srcCode)
	if err != nil {
		b.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	// not Smoke-Tested
	transFunction := jsEngine.MakeFunction("identity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_JSEngine_Passthrough(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x) {
   return x;
}
`
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		RunTimeout: 5,
	}

	jsEngine, err := NewJSEngine(jsConfig, srcCode)
	if err != nil {
		b.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	// not Smoke-Tested
	transFunction := jsEngine.MakeFunction("identity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_JSEngine_PassthroughSpMode(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x) {
   return x;
}
`
	inputMsg := &models.Message{
		Data:         testJsTsv,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		RunTimeout: 5,
	}

	jsEngine, err := NewJSEngine(jsConfig, srcCode)
	if err != nil {
		b.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	// not Smoke-Tested
	transFunction := jsEngine.MakeFunction("identity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_JSEngine_Passthrough_JsJson(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x) {
   var jsonObj = JSON.parse(x.Data);
   var result = JSON.stringify(jsonObj);

   return {
       Data: result
   };
}
`
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		RunTimeout: 5,
	}

	jsEngine, err := NewJSEngine(jsConfig, srcCode)
	if err != nil {
		b.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	// not Smoke-Tested
	transFunction := jsEngine.MakeFunction("jsonIdentity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

// Helper function to compare messages and avoid using reflect.DeepEqual
// on errors. Compares all but the error field of messages.
func assertMessagesCompareJs(t *testing.T, act, exp *models.Message, isJSON bool) {
	t.Helper()

	ok := false
	headersOk := false
	switch {
	case act == nil:
		ok = exp == nil
	case exp == nil:
	default:
		var dataOk bool
		pkOk := act.PartitionKey == exp.PartitionKey
		if isJSON {
			dataOk = assert.JSONEq(t, string(exp.Data), string(act.Data))
		} else {
			dataOk = reflect.DeepEqual(act.Data, exp.Data)
		}
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
		if headersOk == false {
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

// helper variables
var testJsDvceCreatedTstamp, _ = time.Parse("2006-01-02 15:04:05.999", "2019-05-10 14:40:35.551")
var testJsEtlTstamp, _ = time.Parse("2006-01-02 15:04:05.999", "2019-05-10 14:40:37.436")
var testJsDerivedTstamp, _ = time.Parse("2006-01-02 15:04:05.999", "2019-05-10 14:40:35.972")
var testJsCollectorTstamp, _ = time.Parse("2006-01-02 15:04:05.999", "2019-05-10 14:40:35.972")
var testJsDvceSentTstamp, _ = time.Parse("2006-01-02 15:04:05.999", "2019-05-10 14:40:35")
var testJSMap = map[string]interface{}{
	"event_version":       "1-0-0",
	"app_id":              "test-data<>",
	"dvce_created_tstamp": testJsDvceCreatedTstamp,
	"event":               "unstruct",
	"v_collector":         "ssc-0.15.0-googlepubsub",
	"network_userid":      "d26822f5-52cc-4292-8f77-14ef6b7a27e2",
	"event_name":          "add_to_cart",
	"event_vendor":        "com.snowplowanalytics.snowplow",
	"event_format":        "jsonschema",
	"platform":            "pc",
	"etl_tstamp":          testJsEtlTstamp,
	"collector_tstamp":    testJsCollectorTstamp,
	"user_id":             "user<built-in function input>",
	"dvce_sent_tstamp":    testJsDvceSentTstamp,
	"derived_tstamp":      testJsDerivedTstamp,
	"event_id":            "e9234345-f042-46ad-b1aa-424464066a33",
	"v_tracker":           "py-0.8.2",
	"v_etl":               "beam-enrich-0.2.0-common-0.36.0",
	"user_ipaddress":      "1.2.3.4",
	"unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1": map[string]interface{}{
		"quantity":  float64(2),
		"unitPrice": 32.4,
		"currency":  "GBP",
		"sku":       "item41",
	},
	"contexts_nl_basjes_yauaa_context_1": []interface{}{
		map[string]interface{}{
			"deviceName":               "Unknown",
			"layoutEngineVersionMajor": "??",
			"operatingSystemName":      "Unknown",
			"deviceClass":              "Unknown",
			"agentVersion":             "2.21.0",
			"layoutEngineName":         "Unknown",
			"layoutEngineClass":        "Unknown",
			"agentName":                "python-requests",
			"agentNameVersion":         "python-requests 2.21.0",
			"operatingSystemVersion":   "??",
			"agentClass":               "Special",
			"deviceBrand":              "Unknown",
			"agentVersionMajor":        "2",
			"agentNameVersionMajor":    "python-requests 2",
			"operatingSystemClass":     "Unknown",
			"layoutEngineVersion":      "??",
		},
	},
	"useragent": "python-requests/2.21.0",
}

var testJsTsv = []byte(`test-data<>	pc	2019-05-10 14:40:37.436	2019-05-10 14:40:35.972	2019-05-10 14:40:35.551	unstruct	e9234345-f042-46ad-b1aa-424464066a33			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	1.2.3.4				d26822f5-52cc-4292-8f77-14ef6b7a27e2																																									{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0","data":{"sku":"item41","quantity":2,"unitPrice":32.4,"currency":"GBP"}}}																			python-requests/2.21.0																																										2019-05-10 14:40:35.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:35.972	com.snowplowanalytics.snowplow	add_to_cart	jsonschema	1-0-0		`)

// corresponding JSON to previous TSV
var testJsJSON = []byte(`{"app_id":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// json's changed and stringified inside JS
var testJsJSONChanged1 = []byte(`{"app_id_CHANGED":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

var testJsJSONChanged2 = []byte(`{"collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2","app_id_CHANGED":"test-data<>"}`)
