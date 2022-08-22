// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.
//
package engine

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func TestJSLayer(t *testing.T) {
	assert := assert.New(t)

	jsEngine, err := NewJSEngine(&JSEngineConfig{
		SourceB64:         "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ",
		RunTimeout:        15,
		DisableSourceMaps: true,
		SpMode:            false,
	})
	assert.NotNil(t, jsEngine)
	assert.Nil(err)
}

func TestJSEngineMakeFunction_SpModeFalse_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode = false
	testCases := []struct {
		Src               string
		Scenario          string
		DisableSourceMaps bool
		Input             *models.Message
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Src: `
function main(x) {
   return x;
}
`,
			Scenario:          "identity",
			DisableSourceMaps: true,
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
			Scenario:          "concatHello",
			DisableSourceMaps: true,
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
			Scenario:          "filterIn",
			DisableSourceMaps: true,
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
			Scenario:          "filterOut",
			DisableSourceMaps: true,
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
			Scenario:          "jsonIdentity",
			DisableSourceMaps: false,
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
			Error: nil,
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
			Scenario:          "jsonTransformFieldNameRegex",
			DisableSourceMaps: false,
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
			Error: nil,
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
			Scenario:          "jsonTransformFieldNameObj",
			DisableSourceMaps: false,
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
			Error: nil,
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
			Scenario:          "jsonFilterOut",
			DisableSourceMaps: false,
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
			Error:         nil,
		},
		{
			Src: `
function main(x) {
   return 0;
}
`,
			Scenario:          "returnWrongType",
			DisableSourceMaps: true,
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
			Scenario:          "returnUndefined",
			DisableSourceMaps: true,
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
			Scenario:          "returnNull",
			DisableSourceMaps: true,
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
			Scenario:          "causeRuntimeError",
			DisableSourceMaps: true,
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
			Scenario:          "callError",
			DisableSourceMaps: false,
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
			Scenario:          "sleepTenSecs",
			DisableSourceMaps: false,
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

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"])
			assertMessagesCompareJs(t, f, tt.Expected["filtered"])
			assertMessagesCompareJs(t, e, tt.Expected["failed"])
		})
	}
}

func TestJSEngineMakeFunction_SpModeTrue_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = true
	testCases := []struct {
		Scenario          string
		Src               string
		DisableSourceMaps bool
		Input             *models.Message
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Scenario: "identity",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
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
			DisableSourceMaps: true,
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
			DisableSourceMaps: true,
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
			DisableSourceMaps: false,
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
			DisableSourceMaps: true,
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

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"])
			assertMessagesCompareJs(t, f, tt.Expected["filtered"])
			assertMessagesCompareJs(t, e, tt.Expected["failed"])
		})
	}
}

func TestJSEngineMakeFunction_IntermediateState_SpModeFalse(t *testing.T) {
	testSpMode := false
	testCases := []struct {
		Scenario          string
		Src               string
		DisableSourceMaps bool
		Input             *models.Message
		InterState        interface{}
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Scenario: "intermediateState_EngineProtocol_Map",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_spMode_true",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_spMode_false",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"])
			assertMessagesCompareJs(t, f, tt.Expected["filtered"])
			assertMessagesCompareJs(t, e, tt.Expected["failed"])
		})
	}
}

func TestJSEngineMakeFunction_IntermediateState_SpModeTrue(t *testing.T) {
	testSpMode := true
	testCases := []struct {
		Scenario          string
		Src               string
		DisableSourceMaps bool
		Input             *models.Message
		InterState        interface{}
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Scenario: "intermediateState_EngineProtocol_Map",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
		{
			Scenario: "intermediateState_notEngineProtocol_notSpEnriched",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error:         fmt.Errorf("Cannot parse"),
		},
		{
			Scenario: "intermediateState_notEngineProtocol_SpEnriched",
			Src: `
function main(x) {
   return x;
}
`,
			DisableSourceMaps: true,
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
			Error: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"])
			assertMessagesCompareJs(t, f, tt.Expected["filtered"])
			assertMessagesCompareJs(t, e, tt.Expected["failed"])
		})
	}
}

func TestJSEngineMakeFunction_SetPK(t *testing.T) {
	var testInterState interface{} = nil
	testCases := []struct {
		Scenario          string
		Src               string
		DisableSourceMaps bool
		SpMode            bool
		Input             *models.Message
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Scenario: "onlySetPk_spModeTrue",
			Src: `
function main(x) {
   x.PartitionKey = "newPk";
   return x;
}
`,
			DisableSourceMaps: true,
			SpMode:            true,
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
			Error: nil,
		},
		{
			Scenario: "onlySetPk_spModeFalse",
			Src: `
function main(x) {
   x.PartitionKey = "newPk";
   return x;
}
`,
			DisableSourceMaps: true,
			SpMode:            false,
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
			DisableSourceMaps: true,
			SpMode:            true,
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

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            tt.SpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"])
			assertMessagesCompareJs(t, f, tt.Expected["filtered"])
			assertMessagesCompareJs(t, e, tt.Expected["failed"])
		})
	}
}

func TestJSEngineSmokeTest(t *testing.T) {
	testCases := []struct {
		Src               string
		FunName           string
		DisableSourceMaps bool
		CompileError      error
		SmokeError        error
	}{
		{
			Src: `
function identity(x) {
   return x;
}
`,
			FunName:           "identity",
			DisableSourceMaps: true,
			CompileError:      nil,
			SmokeError:        nil,
		},
		{
			Src: `
function notMain(x) {
   return x;
}
`,
			FunName:           "notExists",
			DisableSourceMaps: true,
			CompileError:      nil,
			SmokeError:        fmt.Errorf("could not assert as function"),
		},
		{
			Src: `
function main(x) {
   local y = 0;
}
`,
			FunName:           "syntaxError",
			DisableSourceMaps: false,
			CompileError:      fmt.Errorf("SyntaxError"),
			SmokeError:        nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.FunName, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
			}

			jsEngine, compileErr := NewJSEngine(jsConfig)

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

func TestJSEngine_Examples(t *testing.T) {
	testSpMode := true
	testCases := []struct {
		ExampleFile       string
		DisableSourceMaps bool
		Input             *models.Message
		InterState        interface{}
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			ExampleFile:       "amplitude.js",
			DisableSourceMaps: true,
			Input: &models.Message{
				Data:         testJsTsvExample,
				PartitionKey: "some-test-key",
			},
			InterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         testJsJSONAmplitude,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMapAmplitude,
			},
			Error: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.ExampleFile, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join("examples", tt.ExampleFile)
			fileSrc, err := os.ReadFile(filename)
			if err != nil {
				t.Fatalf("failed to read from example file")
			}

			src := base64.StdEncoding.EncodeToString(fileSrc)
			jsConfig := &JSEngineConfig{
				SourceB64:         src,
				RunTimeout:        5,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := NewJSEngine(jsConfig)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"])
			assertMessagesCompareJs(t, f, tt.Expected["filtered"])
			assertMessagesCompareJs(t, e, tt.Expected["failed"])
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
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: true,
	}

	jsEngine, err := NewJSEngine(jsConfig)
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
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: false,
	}

	jsEngine, err := NewJSEngine(jsConfig)
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
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsTsv,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: false,
	}

	jsEngine, err := NewJSEngine(jsConfig)
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
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &JSEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: false,
	}

	jsEngine, err := NewJSEngine(jsConfig)
	if err != nil {
		b.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	// not Smoke-Tested
	transFunction := jsEngine.MakeFunction("jsonIdentity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func testJSEngineFunc(c *JSEngineConfig) (*JSEngineConfig, error) {
	return c, nil
}

// Helper function to compare messages and avoid using reflect.DeepEqual
// on errors. Compares all but the error field of messages.
func assertMessagesCompareJs(t *testing.T, act, exp *models.Message) {
	t.Helper()

	ok := false
	switch {
	case act == nil:
		ok = exp == nil
	case exp == nil:
	default:
		var dataOk bool
		pkOk := act.PartitionKey == exp.PartitionKey
		dataOk = reflect.DeepEqual(act.Data, exp.Data)
		cTimeOk := reflect.DeepEqual(act.TimeCreated, exp.TimeCreated)
		pTimeOk := reflect.DeepEqual(act.TimePulled, exp.TimePulled)
		tTimeOk := reflect.DeepEqual(act.TimeTransformed, exp.TimeTransformed)
		ackOk := reflect.DeepEqual(act.AckFunc, exp.AckFunc)

		if pkOk && dataOk && cTimeOk && pTimeOk && tTimeOk && ackOk {
			ok = true
		}
	}

	if !ok {
		t.Errorf("\nGOT:\n%s\nEXPECTED:\n%s\n",
			spew.Sdump(act),
			spew.Sdump(exp))
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

//
// corresponding JSON to previous TSV
var testJsJSON = []byte(`{"app_id":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// json's changed and stringified inside JS
var testJsJSONChanged1 = []byte(`{"app_id_CHANGED":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

var testJsJSONChanged2 = []byte(`{"collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2","app_id_CHANGED":"test-data<>"}`)

// Events for examples
var testJsTsvExample = []byte(`media-test	web	2022-07-23 09:18:48.426	2022-07-23 09:18:48.426	2022-07-23 09:18:48.426	unstruct	e9234345-f042-46ad-b1aa-424464066a33			js-3.5.0	snowplow-micro-1.3.1-stdout	snowplow-micro-1.3.1-common-3.1.3	tester	1.2.3.4				3c5154e7-0ba5-4778-a5c4-d38369dea6bc												http://localhost:8000/	Testing																												{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/media_player_event/jsonschema/1-0-0","data":{"type":"play"}}}																			Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36																																										2022-07-23 09:18:48.426			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0","data":[{"schema":"iglu:com.youtube/youtube/jsonschema/1-0-0","data":{"autoPlay":false,"avaliablePlaybackRates":[0.25,0.5,0.75,1,1.25,1.5,1.75,2],"buffering":false,"controls":true,"cued":false,"loaded":3,"playbackQuality":"medium","playerId":"youtube-song","unstarted":false,"url":"https://www.youtube.com/watch?v=foobarbaz","avaliableQualityLevels":["hd1080","hd720","large","medium","small","tiny","auto"]}},{"schema":"iglu:com.snowplowanalytics.snowplow/media_player/jsonschema/1-0-0","data":{"currentTime":0.015303093460083008,"duration":190.301,"ended":false,"loop":false,"muted":false,"paused":false,"playbackRate":1,"volume":100}},{"schema":"iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/1-0-0","data":{"id":"68027aa2-34b1-4018-95e3-7176c62dbc84"}},{"schema":"iglu:com.google.tag-manager.server-side/user_data/jsonschema/1-0-0","data":{"email_address":"foo@test.io"}},{"schema":"iglu:com.snowplowanalytics.snowplow/client_session/jsonschema/1-0-2","data":{"userId":"fd0e5288-e89b-45df-aad5-6d0c6eda6198","sessionId":"1ab28b79-bfdd-4855-9bf1-5199ce15beac","eventIndex":24,"sessionIndex":1,"previousSessionId":null,"storageMechanism":"COOKIE_1","firstEventId":"40fbdb30-1b99-42a3-99f7-850dacf5be43","firstEventTimestamp":"2022-07-23T09:08:04.451Z"}}]}		2022-07-23 09:18:48.426	com.snowplowanalytics.snowplow	media_player_event	jsonschema	1-0-0		`)

var testJsJSONAmplitude = []byte(`{"api_key":"12345","events":[{"event_properties":{"media_event_type":"play","media_player":{"currentTime":0.015303093460083008,"duration":190.301,"ended":false,"loop":false,"muted":false,"paused":false,"playbackRate":1,"volume":100},"page_location":"http://localhost:8000/","page_title":"Testing","youtube":{"autoPlay":false,"avaliablePlaybackRates":[0.25,0.5,0.75,1,1.25,1.5,1.75,2],"avaliableQualityLevels":["hd1080","hd720","large","medium","small","tiny","auto"],"buffering":false,"controls":true,"cued":false,"loaded":3,"playbackQuality":"medium","playerId":"youtube-song","unstarted":false,"url":"https://www.youtube.com/watch?v=foobarbaz"}},"event_type":"media_player_event","insert_id":"e9234345-f042-46ad-b1aa-424464066a33","platform":"web","session_id":1658567284451,"time":1658567928425,"user_id":"tester","user_properties":{"email":"foo@test.io","email_address":"foo@test.io","user_data":{"email_address":"foo@test.io"}}}]}`)

var testJSMapAmplitude = map[string]interface{}{
	"api_key": "12345",
	"events": []interface{}{
		map[string]interface{}{
			"platform":   "web",
			"insert_id":  "e9234345-f042-46ad-b1aa-424464066a33",
			"user_id":    "tester",
			"event_type": "media_player_event",
			"time":       int64(1658567928425),
			"session_id": int64(1658567284451),
			"user_properties": map[string]interface{}{
				"email_address": "foo@test.io",
				"email":         "foo@test.io",
				"user_data": map[string]interface{}{
					"email_address": "foo@test.io",
				},
			},
			"event_properties": map[string]interface{}{
				"media_event_type": "play",
				"page_location":    "http://localhost:8000/",
				"page_title":       "Testing",
				"youtube": map[string]interface{}{
					"avaliablePlaybackRates": []interface{}{
						0.25,
						0.5,
						0.75,
						float64(1),
						1.25,
						1.5,
						1.75,
						float64(2),
					},
					"controls":        true,
					"cued":            false,
					"playerId":        "youtube-song",
					"url":             "https://www.youtube.com/watch?v=foobarbaz",
					"autoPlay":        false,
					"buffering":       false,
					"loaded":          float64(3),
					"playbackQuality": "medium",
					"unstarted":       false,
					"avaliableQualityLevels": []interface{}{
						"hd1080",
						"hd720",
						"large",
						"medium",
						"small",
						"tiny",
						"auto",
					},
				},
				"media_player": map[string]interface{}{
					"currentTime":  0.015303093460083008,
					"duration":     190.301,
					"ended":        false,
					"loop":         false,
					"muted":        false,
					"paused":       false,
					"playbackRate": float64(1),
					"volume":       float64(100),
				},
			},
		},
	},
}
