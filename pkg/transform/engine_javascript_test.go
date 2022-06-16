// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"encoding/base64"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func TestJSEngineConfig_ENV(t *testing.T) {
	testCases := []struct {
		Name     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			Name: "transform-js-from-env",
			Plug: testJSEngineAdapter(testJSEngineFunc),
			Expected: &jsEngineConfig{
				SourceB64:         "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ",
				RunTimeout:        10,
				DisableSourceMaps: false,
				SpMode:            false,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", "")

			t.Setenv("MESSAGE_TRANSFORMATION", "js")
			t.Setenv("TRANSFORMATION_LAYER_NAME", "js")

			t.Setenv("TRANSFORMATION_JS_SOURCE_B64", "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ")
			t.Setenv("TRANSFORMATION_JS_TIMEOUT_SEC", "10")
			t.Setenv("TRANSFORMATION_JS_DISABLE_SOURCE_MAPS", "false")
			t.Setenv("TRANSFORMATION_JS_SNOWPLOW_MODE", "false")

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			engine := c.Data.Transform.Layer
			decoderOpts := &config.DecoderOptions{
				Input: engine.Body,
			}

			result, err := c.CreateComponent(tt.Plug, decoderOpts)
			assert.NotNil(result)
			assert.Nil(err)

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestJSEngineConfig_HCL(t *testing.T) {
	testFixPath := "../../config/test-fixtures"
	testCases := []struct {
		File     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			File: "transform-js-simple.hcl",
			Plug: testJSEngineAdapter(testJSEngineFunc),
			Expected: &jsEngineConfig{
				SourceB64:         "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ",
				RunTimeout:        5,
				DisableSourceMaps: true,
				SpMode:            false,
			},
		},
		{
			File: "transform-js-extended.hcl",
			Plug: testJSEngineAdapter(testJSEngineFunc),
			Expected: &jsEngineConfig{
				SourceB64:         "CglmdW5jdGlvbiBmb28oeCkgewoJICAgIHJldHVybiB4OwoJfQoJ",
				RunTimeout:        10,
				DisableSourceMaps: false,
				SpMode:            true,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(testFixPath, tt.File)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			engine := c.Data.Transform.Layer
			decoderOpts := &config.DecoderOptions{
				Input: engine.Body,
			}

			result, err := c.CreateComponent(tt.Plug, decoderOpts)
			assert.NotNil(result)
			assert.Nil(err)

			if !reflect.DeepEqual(result, tt.Expected) {
				t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(result),
					spew.Sdump(tt.Expected))
			}
		})
	}
}

func TestJSLayer(t *testing.T) {
	layer := JSLayer()
	if _, ok := layer.(config.Pluggable); !ok {
		t.Errorf("invalid interface returned from JSLayer")
	}
}

func TestJSEngineMakeFunction_SpModeFalse_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = false
	testCases := []struct {
		Src               string
		FunName           string
		DisableSourceMaps bool
		Input             *models.Message
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Src: `
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         "asdf",
			},
			Error: nil,
		},
		{
			Src: `
function concatHello(x) {
    let newVal = "Hello:" + x.Data;
    x.Data = newVal;
    return x;
}
`,
			FunName:           "concatHello",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         "Hello:asdf",
			},
			Error: nil,
		},
		{
			Src: `
function filterIn(x) {
    x.FilterOut = false
    return x;
}
`,
			FunName:           "filterIn",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         "asdf",
			},
			Error: nil,
		},
		{
			Src: `
function filterOut(x) {
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
			FunName:           "filterOut",
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
function jsonIdentity(x) {
    var jsonObj = JSON.parse(x.Data);
    var result = JSON.stringify(jsonObj);

    return {
        Data: result
    };
}
`,
			FunName:           "jsonIdentity",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			Error: nil,
		},
		{
			Src: `
function jsonTransformFieldNameRegex(x) {
    var jsonObj = JSON.parse(x.Data);

    if (jsonObj.hasOwnProperty("app_id")) {
        x.Data = x.Data.replace(/app_id/, 'app_id_CHANGED');
    }

    return x;
}
`,
			FunName:           "jsonTransformFieldNameRegex",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSONChanged1),
			},
			Error: nil,
		},
		{
			Src: `
function jsonTransformFieldNameObj(x) {

    var jsonObj = JSON.parse(x.Data);

    var descriptor = Object.getOwnPropertyDescriptor(jsonObj, "app_id");
    Object.defineProperty(jsonObj, "app_id_CHANGED", descriptor);
    delete jsonObj["app_id"];

    return {
        Data: JSON.stringify(jsonObj)
    };
}
`,
			FunName:           "jsonTransformFieldNameObj",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSONChanged2),
			},
			Error: nil,
		},
		{
			Src: `
function jsonFilterOut(x) {
    var jsonObj = JSON.parse(x.Data);

    if (jsonObj.hasOwnProperty("app_id") && jsonObj["app_id"] === "filterMeOut") {
        x.FilterOut = false;
    } else {
        x.FilterOut = true;
    }

    return x;
}
`,
			FunName:           "jsonFilterOut",
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
function returnWrongType(x) {
    return 0;
}
`,
			FunName:           "returnWrongType",
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
function returnUndefined(x) {}
`,
			FunName:           "returnUndefined",
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
function returnNull(x) {
  return null;
}
`,
			FunName:           "returnNull",
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
function causeRuntimeError(x) {
    return x.toExponential(2);
}
`,
			FunName:           "causeRuntimeError",
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
			Error:         fmt.Errorf("error running JavaScript function \"causeRuntimeError\""),
		},
		{
			Src: `
function callError(x) {
    throw("Failed");
}
`,
			FunName:           "callError",
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
			Error:         fmt.Errorf("error running JavaScript function \"callError\""),
		},
		{
			Src: `
function sleepTenSecs(x) {
    var now = new Date().getTime();
    while(new Date().getTime() < now + 10000) {
    }
}
`,
			FunName:           "sleepTenSecs",
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
		t.Run(tt.FunName, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			jsConfig := &jsEngineConfig{
				SourceB64:         src,
				RunTimeout:        1,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := newJSEngine(jsConfig)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function newJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], false)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], false)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], false)
		})
	}
}

func TestJSEngineMakeFunction_SpModeTrue_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = true
	testCases := []struct {
		Scenario          string
		Src               string
		FunName           string
		DisableSourceMaps bool
		Input             *models.Message
		Expected          map[string]*models.Message
		ExpInterState     interface{}
		Error             error
	}{
		{
			Scenario: "identity",
			Src: `
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			Error: nil,
		},
		{
			Scenario: "filtering",
			Src: `
function filterOut(input) {
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
			FunName:           "filterOut",
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
function filterOutIgnores(x) {
    return {
        FilterOut: true,
        Data: "shouldNotAppear",
        PartitionKey: "notThis"
    };
}
`,
			FunName:           "filterOutIgnores",
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
function willNotRun(x) {
   return x;
}
`,
			FunName:           "willNotRun",
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
function returnWrongType(x) {
    return 0;
}
`,
			FunName:           "returnWrongType",
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
			jsConfig := &jsEngineConfig{
				SourceB64:         src,
				RunTimeout:        1,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := newJSEngine(jsConfig)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function newJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], true)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], false)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], false)
		})
	}
}

func TestJSEngineMakeFunction_IntermediateState_SpModeFalse(t *testing.T) {
	testSpMode := false
	testCases := []struct {
		Scenario          string
		Src               string
		FunName           string
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
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
			DisableSourceMaps: true,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
			DisableSourceMaps: true,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_spMode_true",
			Src: `
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_spMode_false",
			Src: `
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
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
			ExpInterState: &EngineProtocol{
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
			jsConfig := &jsEngineConfig{
				SourceB64:         src,
				RunTimeout:        1,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := newJSEngine(jsConfig)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function newJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], true)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], true)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], true)
		})
	}
}

func TestJSEngineMakeFunction_IntermediateState_SpModeTrue(t *testing.T) {
	testSpMode := true
	testCases := []struct {
		Scenario          string
		Src               string
		FunName           string
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
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
			DisableSourceMaps: true,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testJSMap,
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
			DisableSourceMaps: true,
			Input: &models.Message{
				Data:         testJsJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testJsJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_notEngineProtocol_notSpEnriched",
			Src: `
function willNotRun(x) {
    return x;
}
`,
			FunName:           "willNotRun",
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
function identity(x) {
    return x;
}
`,
			FunName:           "identity",
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
			ExpInterState: &EngineProtocol{
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
			jsConfig := &jsEngineConfig{
				SourceB64:         src,
				RunTimeout:        1,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            testSpMode,
			}

			jsEngine, err := newJSEngine(jsConfig)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function newJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], true)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], true)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], true)
		})
	}
}

func TestJSEngineMakeFunction_SetPK(t *testing.T) {
	var testInterState interface{} = nil
	testCases := []struct {
		Scenario          string
		Src               string
		FunName           string
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
function onlySetPk(x) {
    x.PartitionKey = "newPk";
    return x;
}
`,
			FunName:           "onlySetPk",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         testJSMap,
			},
			Error: nil,
		},
		{
			Scenario: "onlySetPk_spModeFalse",
			Src: `
function onlySetPk(x) {
    x.PartitionKey = "newPk";
    return x;
}
`,
			FunName:           "onlySetPk",
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
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         string(testJsTsv),
			},
			Error: nil,
		},
		{
			Scenario: "filterOutIgnores",
			Src: `
function filterOutIgnores(x) {
    return {
        FilterOut: true,
        Data: "shouldNotAppear",
        PartitionKey: "notThis"
    };
}
`,
			FunName:           "filterOutIgnores",
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
			jsConfig := &jsEngineConfig{
				SourceB64:         src,
				RunTimeout:        1,
				DisableSourceMaps: tt.DisableSourceMaps,
				SpMode:            tt.SpMode,
			}

			jsEngine, err := newJSEngine(jsConfig)
			assert.NotNil(jsEngine)
			if err != nil {
				t.Fatalf("function newJSEngine failed with error: %q", err.Error())
			}

			if err := jsEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := jsEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareJs(t, s, tt.Expected["success"], false)
			assertMessagesCompareJs(t, f, tt.Expected["filtered"], false)
			assertMessagesCompareJs(t, e, tt.Expected["failed"], false)
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
function notThisOne(x) {
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
function syntaxError(x) {
    loca y = 0;
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
			jsConfig := &jsEngineConfig{
				SourceB64:         src,
				RunTimeout:        1,
				DisableSourceMaps: tt.DisableSourceMaps,
			}

			jsEngine, compileErr := newJSEngine(jsConfig)

			if compileErr != nil {
				if tt.CompileError == nil {
					t.Fatalf("got unexpected error while creating newJSEngine: %s", compileErr.Error())
				}

				if !strings.Contains(compileErr.Error(), tt.CompileError.Error()) {
					t.Errorf("newJSEngine error mismatch\nGOT_ERROR:\n%q\n does not contain\nEXPECTED_ERROR:\n%q",
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

func TestJSEngineWithBuiltinsSpModeFalse(t *testing.T) {
	srcCode := `
function identity(x) {
    return x;
}

function setPk(x) {
    x.PartitionKey = "testKey";
    return x;
}
`
	// JS
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	jsConfig := &jsEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		SpMode:     false,
	}

	jsEngine, err := newJSEngine(jsConfig)
	if err != nil {
		t.Fatalf("newJSEngine failed with error: %q", err)
	}

	if err := jsEngine.SmokeTest("identity"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}
	if err := jsEngine.SmokeTest("setPk"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	jsFuncID := jsEngine.MakeFunction("identity")
	jsFuncPk := jsEngine.MakeFunction("setPk")

	// Builtins
	setPkToAppID := NewSpEnrichedSetPkFunction("app_id")
	spEnrichedToJSON := SpEnrichedToJSON

	testCases := []struct {
		Name           string
		Transformation TransformationApplyFunction
		Input          []*models.Message
		ExpectedGood   []*models.Message
	}{
		{
			Name:  "identity0",
			Input: messages,
			Transformation: NewTransformation(
				jsFuncID,
				setPkToAppID,
				spEnrichedToJSON,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         snowplowJSON1,
					PartitionKey: "test-data1",
				},
				{
					Data:         snowplowJSON2,
					PartitionKey: "test-data2",
				},
				{
					Data:         snowplowJSON3,
					PartitionKey: "test-data3",
				},
			},
		},
		{
			Name:  "identity2",
			Input: messages,
			Transformation: NewTransformation(
				setPkToAppID,
				spEnrichedToJSON,
				jsFuncID,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         snowplowJSON1,
					PartitionKey: "test-data1",
				},
				{
					Data:         snowplowJSON2,
					PartitionKey: "test-data2",
				},
				{
					Data:         snowplowJSON3,
					PartitionKey: "test-data3",
				},
			},
		},
		{
			Name:  "setPk1",
			Input: messages,
			Transformation: NewTransformation(
				setPkToAppID,
				jsFuncPk,
				spEnrichedToJSON,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         snowplowJSON1,
					PartitionKey: "testKey",
				},
				{
					Data:         snowplowJSON2,
					PartitionKey: "testKey",
				},
				{
					Data:         snowplowJSON3,
					PartitionKey: "testKey",
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			result := tt.Transformation(tt.Input)
			assert.NotNil(result)
			assert.Equal(len(tt.ExpectedGood), len(result.Result))
			for i, res := range result.Result {
				if i < len(tt.ExpectedGood) {
					exp := tt.ExpectedGood[i]
					assert.JSONEq(string(exp.Data), string(res.Data))
					assert.Equal(exp.PartitionKey, res.PartitionKey)
				}
			}
		})
	}
}

func TestJSEngineWithBuiltinsSpModeTrue(t *testing.T) {
	srcCode := `
function identity(x) {
    return x;
}

function setPk(x) {
    x.PartitionKey = "testKey";
    return x;
}
`
	// JS
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	jsConfig := &jsEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		SpMode:     true,
	}

	jsEngine, err := newJSEngine(jsConfig)
	if err != nil {
		t.Fatalf("newJSEngine failed with error: %q", err)
	}

	if err := jsEngine.SmokeTest("identity"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}
	if err := jsEngine.SmokeTest("setPk"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	jsFuncID := jsEngine.MakeFunction("identity")
	jsFuncPk := jsEngine.MakeFunction("setPk")

	// Builtins
	setPkToAppID := NewSpEnrichedSetPkFunction("app_id")
	spEnrichedToJSON := SpEnrichedToJSON

	testCases := []struct {
		Name           string
		Transformation TransformationApplyFunction
		Input          []*models.Message
		ExpectedGood   []*models.Message
	}{
		{
			Name: "identity",
			Input: []*models.Message{
				{
					Data:         testJsTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: NewTransformation(
				setPkToAppID,
				spEnrichedToJSON,
				jsFuncID,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         testJsJSON,
					PartitionKey: "test-data<>",
				},
			},
		},
		{
			Name: "setPk",
			Input: []*models.Message{
				{
					Data:         testJsTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: NewTransformation(
				setPkToAppID,
				jsFuncPk,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         testJsJSON,
					PartitionKey: "testKey",
				},
			},
		},
		{
			Name: "mix",
			Input: []*models.Message{
				{
					Data:         testJsTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: NewTransformation(
				setPkToAppID,
				jsFuncID,
				jsFuncPk,
				jsFuncID,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         testJsJSON,
					PartitionKey: "testKey",
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			result := tt.Transformation(tt.Input)
			assert.NotNil(result)
			assert.Equal(len(tt.ExpectedGood), len(result.Result))
			for i, res := range result.Result {
				if i < len(tt.ExpectedGood) {
					exp := tt.ExpectedGood[i]
					assert.JSONEq(string(exp.Data), string(res.Data))
					assert.Equal(exp.PartitionKey, res.PartitionKey)
				}
			}
		})
	}
}

func Benchmark_JSEngine_Passthrough_DisabledSrcMaps(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function identity(x) {
    return x;
}
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &jsEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: true,
	}

	jsEngine, err := newJSEngine(jsConfig)
	if err != nil {
		b.Fatalf("function newJSEngine failed with error: %q", err.Error())
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
function identity(x) {
    return x;
}
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsJSON,
		PartitionKey: "some-test-key",
	}

	jsConfig := &jsEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: false,
	}

	jsEngine, err := newJSEngine(jsConfig)
	if err != nil {
		b.Fatalf("function newJSEngine failed with error: %q", err.Error())
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
function identity(x) {
    return x;
}
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	inputMsg := &models.Message{
		Data:         testJsTsv,
		PartitionKey: "some-test-key",
	}

	jsConfig := &jsEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: false,
	}

	jsEngine, err := newJSEngine(jsConfig)
	if err != nil {
		b.Fatalf("function newJSEngine failed with error: %q", err.Error())
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
function jsonIdentity(x) {
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

	jsConfig := &jsEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: false,
	}

	jsEngine, err := newJSEngine(jsConfig)
	if err != nil {
		b.Fatalf("function newJSEngine failed with error: %q", err.Error())
	}

	// not Smoke-Tested
	transFunction := jsEngine.MakeFunction("jsonIdentity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

// Test helpers
func testJSEngineAdapter(f func(c *jsEngineConfig) (*jsEngineConfig, error)) jsEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*jsEngineConfig)
		if !ok {
			return nil, fmt.Errorf("invalid input, expected jsEngineConfig")
		}

		return f(cfg)
	}

}

func testJSEngineFunc(c *jsEngineConfig) (*jsEngineConfig, error) {
	return c, nil
}

// Helper function to compare messages and avoid using reflect.DeepEqual
// on errors. Compares all but the error field of messages.
func assertMessagesCompareJs(t *testing.T, act, exp *models.Message, isJSON bool) {
	t.Helper()

	ok := false
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

// corresponding JSON to previous TSV
var testJsJSON = []byte(`{"app_id":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// json's changed and stringified inside JS
var testJsJSONChanged1 = []byte(`{"app_id_CHANGED":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

var testJsJSONChanged2 = []byte(`{"collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2","app_id_CHANGED":"test-data<>"}`)
