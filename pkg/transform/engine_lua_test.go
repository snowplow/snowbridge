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

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func TestLuaEngineConfig_ENV(t *testing.T) {
	testCases := []struct {
		Name     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			Name: "transform-lua-from-env",
			Plug: testLuaEngineAdapter(testLuaEngineFunc),
			Expected: &luaEngineConfig{
				SourceB64:  "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ",
				RunTimeout: 10,
				Sandbox:    false,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", "")

			t.Setenv("MESSAGE_TRANSFORMATION", "lua:fun")
			t.Setenv("TRANSFORMATION_LAYER_NAME", "lua")

			t.Setenv("TRANSFORMATION_LUA_SOURCE_B64", "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ")
			t.Setenv("TRANSFORMATION_LUA_TIMEOUT_SEC", "10")
			t.Setenv("TRANSFORMATION_LUA_SANDBOX", "false")

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

func TestLuaEngineConfig_HCL(t *testing.T) {
	fixturesDir := "../../config/test-fixtures"
	testCases := []struct {
		File     string
		Plug     config.Pluggable
		Expected interface{}
	}{
		{
			File: "transform-lua-simple.hcl",
			Plug: testLuaEngineAdapter(testLuaEngineFunc),
			Expected: &luaEngineConfig{
				SourceB64:  "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ",
				RunTimeout: 5,
				Sandbox:    true,
			},
		},
		{
			File: "transform-lua-extended.hcl",
			Plug: testLuaEngineAdapter(testLuaEngineFunc),
			Expected: &luaEngineConfig{
				SourceB64:  "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ",
				RunTimeout: 10,
				Sandbox:    false,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.File, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(fixturesDir, tt.File)
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

func TestLuaLayer(t *testing.T) {
	layer := LuaLayer()
	if _, ok := layer.(config.Pluggable); !ok {
		t.Errorf("invalid interface returned from LuaLayer")
	}
}

func TestLuaEngineMakeFunction_SpModeFalse_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = false
	testCases := []struct {
		Src           string
		FunName       string
		Sandbox       bool
		Input         *models.Message
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: true,
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
function concatHello(x)
   x.Data = "Hello:" .. x.Data
   return x
end
`,
			FunName: "concatHello",
			Sandbox: true,
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
function filterIn(x)
   x.FilterOut = false
   return x
end
`,
			FunName: "filterIn",
			Sandbox: true,
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
function filterOut(x)
   if type(x.Data) == "string" then
      return { FilterOut = true }
   end
   return { FilterOut = false }
end
`,
			FunName: "filterOut",
			Sandbox: false,
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
local json = require("json")

function jsonIdentity(x)
   local dat = x["Data"]
   local jsonObj, decodeErr = json.decode(dat)
   if decodeErr then error(decodeErr) end

   local result, encodeErr = json.encode(jsonObj)
   if encodeErr then error(encodeErr) end

   x.Data = result
   return x
end
`,
			FunName: "jsonIdentity",
			Sandbox: false,
			Input: &models.Message{
				Data:         snowplowJSON1,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         snowplowJSON1,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(snowplowJSON1),
			},
			Error: nil,
		},
		{
			Src: `
local json = require("json")

function jsonTransformFieldName(x)
   local data = x["Data"]
   local jsonObj, decodeErr = json.decode(data)
   if decodeErr then error(decodeErr) end

   jsonObj["app_id_CHANGED"] = jsonObj["app_id"]
   jsonObj["app_id"] = nil

   local result, encodeErr = json.encode(jsonObj)
   if encodeErr then error(encodeErr) end

   return { Data = result }
end
`,
			FunName: "jsonTransformFieldName",
			Sandbox: false,
			Input: &models.Message{
				Data:         snowplowJSON1,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         snowplowJSON1ChangedLua,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(snowplowJSON1ChangedLua),
			},
			Error: nil,
		},
		{
			Src: `
local json = require("json")

function jsonFilterOut(x)
   local jsonObj, decodeErr = json.decode(x["Data"])
   if decodeErr then error(decodeErr) end

   if jsonObj["app_id"] == "filterMeOut" then
      return { FilterOut = false, Data = x["Data"] }
   else
      return { FilterOut = true }
   end
end
`,
			FunName: "jsonFilterOut",
			Sandbox: false,
			Input: &models.Message{
				Data:         snowplowJSON1,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         snowplowJSON1,
					PartitionKey: "some-test-key",
				},
				"failed": nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Src: `
function retWrongType(x)
   return 0
end
`,
			FunName: "retWrongType",
			Sandbox: true,
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
			Error:         fmt.Errorf("invalid return type from Lua transformation; expected Lua Table"),
		},
		{
			Src: `
function noReturn(x)
end
`,
			FunName: "noReturn",
			Sandbox: true,
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
			Error:         fmt.Errorf("invalid return type from Lua transformation; expected Lua Table"),
		},
		{
			Src: `
function returnNil(x)
   return nil
end
`,
			FunName: "returnNil",
			Sandbox: true,
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
			Error:         fmt.Errorf("invalid return type from Lua transformation; expected Lua Table"),
		},
		{
			Src: `
function causeRuntimeError(x)
   return 2 * x
end
`,
			FunName: "causeRuntimeError",
			Sandbox: true,
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
			Error:         fmt.Errorf("error running Lua function \"causeRuntimeError\""),
		},
		{
			Src: `
function callError(x)
   error("Failed")
end
`,
			FunName: "callError",
			Sandbox: false,
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
			Error:         fmt.Errorf("error running Lua function \"callError\""),
		},
		{
			Src: `
local clock = os.clock

function sleepTenSecs(x)
   local t0 = clock()
   while clock() - t0 <= 10 do end
end
`,
			FunName: "sleepTenSecs",
			Sandbox: false,
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
			Error:         fmt.Errorf("context deadline exceeded"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.FunName, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			luaConfig := &luaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := newLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function newLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareLua(t, s, tt.Expected["success"])
			assertMessagesCompareLua(t, f, tt.Expected["filtered"])
			assertMessagesCompareLua(t, e, tt.Expected["failed"])
		})
	}
}

func TestLuaEngineMakeFunction_SpModeTrue_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = true
	testCases := []struct {
		Scenario      string
		Src           string
		FunName       string
		Sandbox       bool
		Input         *models.Message
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Scenario: "identity",
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: false,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "filtering",
			Src: `
function filterOut(input)
   -- input is a lua table
   local spData = input["Data"]
   if spData["app_id"] == "myApp" then
      return input;
   end
   return { FilterOut = true }
end
`,
			FunName: "filterOut",
			Sandbox: false,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testLuaTsv,
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
function filterOutIgnores(x)
   local ret = {
      FilterOut = true,
      Data = "shouldNotAppear",
      PartitionKey = "notThis"
   }
   return ret
end
`,
			FunName: "filterOutIgnores",
			Sandbox: false,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testLuaTsv,
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
function willNotRun(x)
   return x
end
`,
			FunName: "willNotRun",
			Sandbox: false,
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
function returnWrongType(x)
   return 0
end
`,
			FunName: "returnWrongType",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "some-test-key",
			},
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         testLuaTsv,
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("invalid return type from Lua transformation; expected Lua Table"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			luaConfig := &luaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := newLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function newLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareLua(t, s, tt.Expected["success"])
			assertMessagesCompareLua(t, f, tt.Expected["filtered"])
			assertMessagesCompareLua(t, e, tt.Expected["failed"])
		})
	}
}

func TestLuaEngineMakeFunction_IntermediateState_SpModeFalse(t *testing.T) {
	testSpMode := false
	testCases := []struct {
		Scenario      string
		Src           string
		FunName       string
		Sandbox       bool
		Input         *models.Message
		InterState    interface{}
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Scenario: "intermediateState_EngineProtocol_Map",
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_nonSpEnriched",
			Src: `
function identity(x)
    return x;
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_SpEnriched",
			Src: `
function identity(x)
    return x;
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaTsv,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaTsv),
			},
			Error: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			luaConfig := &luaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := newLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function newLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareLua(t, s, tt.Expected["success"])
			assertMessagesCompareLua(t, f, tt.Expected["filtered"])
			assertMessagesCompareLua(t, e, tt.Expected["failed"])
		})
	}
}

func TestLuaEngineMakeFunction_IntermediateState_SpModeTrue(t *testing.T) {
	testSpMode := true

	testCases := []struct {
		Scenario      string
		Src           string
		FunName       string
		Sandbox       bool
		Input         *models.Message
		InterState    interface{}
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Scenario: "intermediateState_EngineProtocol_Map",
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_notEngineProtocol_notSpEnriched",
			Src: `
function willNotRun(x)
   return x
end
`,
			FunName: "willNotRun",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
			},
			ExpInterState: nil,
			Error:         fmt.Errorf("Cannot parse"),
		},
		{
			Scenario: "intermediateState_notEngineProtocol_SpEnriched",
			Src: `
function identity(x)
   return x
end
`,
			FunName: "identity",
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "some-test-key",
			},
			InterState: "notEngineProtocol",
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "some-test-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			luaConfig := &luaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := newLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function newLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareLua(t, s, tt.Expected["success"])
			assertMessagesCompareLua(t, f, tt.Expected["filtered"])
			assertMessagesCompareLua(t, e, tt.Expected["failed"])
		})
	}
}

func TestLuaEngineMakeFunction_SetPK(t *testing.T) {
	var testInterState interface{} = nil
	testCases := []struct {
		Scenario      string
		Src           string
		FunName       string
		Sandbox       bool
		SpMode        bool
		Input         *models.Message
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Scenario: "onlySetPk_spModeTrue",
			Src: `
function onlySetPk(x)
    x["PartitionKey"] = "newPk"
    return x
end
`,
			FunName: "onlySetPk",
			Sandbox: true,
			SpMode:  true,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "oldPK",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaJSON,
					PartitionKey: "newPk",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "onlySetPk_spModeFalse",
			Src: `
function onlySetPk(x)
    x["PartitionKey"] = "newPk"
    return x
end
`,
			FunName: "onlySetPk",
			Sandbox: true,
			SpMode:  false,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "oldPK",
			},
			Expected: map[string]*models.Message{
				"success": {
					Data:         testLuaTsv,
					PartitionKey: "newPk",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: &EngineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         string(testLuaTsv),
			},
			Error: nil,
		},
		{
			Scenario: "filterOutIgnores",
			Src: `
function filterOutIgnores(x)
   local ret = {
      FilterOut = true,
      Data = "shouldNotAppear",
      PartitionKey = "notThis"
   }
   return ret
end
`,
			FunName: "filterOutIgnores",
			Sandbox: true,
			SpMode:  true,
			Input: &models.Message{
				Data:         testLuaTsv,
				PartitionKey: "oldPk",
			},
			Expected: map[string]*models.Message{
				"success": nil,
				"filtered": {
					Data:         testLuaTsv,
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
			luaConfig := &luaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     tt.SpMode,
			}

			luaEngine, err := newLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function newLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(tt.FunName); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(tt.FunName)
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

			assertMessagesCompareLua(t, s, tt.Expected["success"])
			assertMessagesCompareLua(t, f, tt.Expected["filtered"])
			assertMessagesCompareLua(t, e, tt.Expected["failed"])
		})
	}
}

func TestLuaEngineSmokeTest(t *testing.T) {
	testCases := []struct {
		Src          string
		FunName      string
		Sandbox      bool
		CompileError error
		SmokeError   error
	}{
		{
			Src: `
function identity(x)
   return x
end
`,
			FunName:      "identity",
			Sandbox:      true,
			CompileError: nil,
			SmokeError:   nil,
		},
		{
			Src: `
function notThisOne(x)
   return "something"
end
`,
			FunName:      "notExists",
			Sandbox:      true,
			CompileError: nil,
			SmokeError:   fmt.Errorf("global Lua function not found"),
		},
		{
			Src: `
local json = require("json")
local clock = os.clock
`,
			FunName:      "notCalledMissingLibs",
			Sandbox:      true,
			CompileError: nil,
			SmokeError:   fmt.Errorf("could not load lua code"),
		},
		{
			Src: `
function syntaxError(x)
   loca y = 0
end
`,
			FunName:      "syntaxError",
			Sandbox:      false,
			CompileError: fmt.Errorf("error"),
			SmokeError:   nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.FunName, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			luaConfig := &luaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
			}

			luaEngine, compileErr := newLuaEngine(luaConfig)

			if compileErr != nil {
				if tt.CompileError == nil {
					t.Fatalf("got unexpected error while creating newLuaEngine: %s", compileErr.Error())
				}

				if !strings.Contains(compileErr.Error(), tt.CompileError.Error()) {
					t.Errorf("newLuaEngine error mismatch\nGOT_ERROR:\n%q\n does not contain\nEXPECTED_ERROR:\n%q",
						compileErr.Error(),
						tt.CompileError.Error())
				}
			} else {
				assert.NotNil(luaEngine)

				smoke := luaEngine.SmokeTest(tt.FunName)
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

func TestLuaEngineWithBuiltins(t *testing.T) {
	var expectedGood = []*models.Message{
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
	}

	srcCode := `
function identity(x)
   return x
end
`
	funcName := "identity"
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	luaConfig := &luaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
	}

	luaEngine, err := newLuaEngine(luaConfig)
	if err != nil {
		t.Fatalf("newLuaEngine failed with error: %q", err)
	}

	if err := luaEngine.SmokeTest(funcName); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	luaFunc := luaEngine.MakeFunction(funcName)
	setPkToAppID := NewSpEnrichedSetPkFunction("app_id")
	spEnrichedToJSON := SpEnrichedToJSON

	testCases := []struct {
		Name           string
		Transformation TransformationApplyFunction
	}{
		{
			Name: "first",
			Transformation: NewTransformation(
				setPkToAppID,
				spEnrichedToJSON,
				luaFunc,
			),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			transformMultiple := tt.Transformation

			result := transformMultiple(messages)
			assert.NotNil(result)
			for i, res := range result.Result {
				exp := expectedGood[i]
				if !reflect.DeepEqual(res.Data, exp.Data) {
					t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
						spew.Sdump(res.Data),
						spew.Sdump(exp.Data))
				}
				assert.Equal(res.PartitionKey, exp.PartitionKey)

			}
		})
	}

}

func TestLuaEngineWithBuiltinsSpModeFalse(t *testing.T) {
	srcCode := `
function identity(x)
   return x
end

function setPk(x)
   x["PartitionKey"] = "testKey"
   return x
end
`
	// Lua
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	luaConfig := &luaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
		SpMode:     false,
	}

	luaEngine, err := newLuaEngine(luaConfig)
	if err != nil {
		t.Fatalf("newLuaEngine failed with error: %q", err)
	}

	if err := luaEngine.SmokeTest("identity"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}
	if err := luaEngine.SmokeTest("setPk"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	luaFuncID := luaEngine.MakeFunction("identity")
	luaFuncPk := luaEngine.MakeFunction("setPk")

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
				luaFuncID,
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
				luaFuncID,
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
				luaFuncPk,
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
					if !reflect.DeepEqual(res.Data, exp.Data) {
						t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
							spew.Sdump(res.Data),
							spew.Sdump(exp.Data))
					}
					assert.Equal(res.PartitionKey, exp.PartitionKey)
				}
			}
		})
	}
}

func TestLuaEngineWithBuiltinsSpModeTrue(t *testing.T) {
	srcCode := `
function identity(x)
   return x
end

function setPk(x)
   x["PartitionKey"] = "testKey"
   return x
end
`
	// Lua
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	luaConfig := &luaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
		SpMode:     true,
	}

	luaEngine, err := newLuaEngine(luaConfig)
	if err != nil {
		t.Fatalf("newLuaEngine failed with error: %q", err)
	}

	if err := luaEngine.SmokeTest("identity"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}
	if err := luaEngine.SmokeTest("setPk"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	luaFuncID := luaEngine.MakeFunction("identity")
	luaFuncPk := luaEngine.MakeFunction("setPk")

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
					Data:         testLuaTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: NewTransformation(
				setPkToAppID,
				spEnrichedToJSON,
				luaFuncID,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         testLuaJSON,
					PartitionKey: "test-data<>",
				},
			},
		},
		{
			Name: "setPk",
			Input: []*models.Message{
				{
					Data:         testLuaTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: NewTransformation(
				setPkToAppID,
				luaFuncPk,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         testLuaJSON,
					PartitionKey: "testKey",
				},
			},
		},
		{
			Name: "mix",
			Input: []*models.Message{
				{
					Data:         testLuaTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: NewTransformation(
				setPkToAppID,
				luaFuncID,
				luaFuncPk,
				luaFuncID,
			),
			ExpectedGood: []*models.Message{
				{
					Data:         testLuaJSON,
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
					if !reflect.DeepEqual(res.Data, exp.Data) {
						t.Errorf("GOT:\n%s\nEXPECTED:\n%s",
							spew.Sdump(res.Data),
							spew.Sdump(exp.Data))
					}
					assert.Equal(res.PartitionKey, exp.PartitionKey)
				}
			}
		})
	}
}

func Benchmark_LuaEngine_Passthrough_Sandboxed(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function identity(x)
   return x
end
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))

	inputMsg := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-test-key",
	}
	luaConfig := &luaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    true,
	}

	luaEngine, err := newLuaEngine(luaConfig)
	if err != nil {
		b.Fatalf("function newLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("identity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_LuaEngine_Passthrough(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function identity(x)
   return x
end
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))

	inputMsg := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-test-key",
	}
	luaConfig := &luaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    false,
	}

	luaEngine, err := newLuaEngine(luaConfig)
	if err != nil {
		b.Fatalf("function newLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("identity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_LuaEngine_Passthrough_Json(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function jsonIdentity(x)
   local jsonObj, _ = json.decode(x)
   local result, _ = json.encode(jsonObj)

   return result
end
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))

	inputMsg := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-test-key",
	}
	luaConfig := &luaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    false,
	}

	luaEngine, err := newLuaEngine(luaConfig)
	if err != nil {
		b.Fatalf("function newLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("jsonIdentity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

// Test helpers
func testLuaEngineAdapter(f func(c *luaEngineConfig) (*luaEngineConfig, error)) luaEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*luaEngineConfig)
		if !ok {
			return nil, fmt.Errorf("invalid input, expected luaEngineConfig")
		}

		return f(cfg)
	}

}

func testLuaEngineFunc(c *luaEngineConfig) (*luaEngineConfig, error) {

	return c, nil
}

// Helper function to compare messages and avoid using reflect.DeepEqual
// on errors. Compares all but the error field of messages.
func assertMessagesCompareLua(t *testing.T, act, exp *models.Message) {
	t.Helper()

	ok := false
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
var testLuaTimes = map[string]string{
	"dvceCreatedTstamp": "2019-05-10T14:40:35.551Z",
	"etlTstamp":         "2019-05-10T14:40:37.436Z",
	"derivedTstamp":     "2019-05-10T14:40:35.972Z",
	"collectorTstamp":   "2019-05-10T14:40:35.972Z",
	"dvceSentTstamp":    "2019-05-10T14:40:35Z",
}

var testLuaMap = map[string]interface{}{
	"event_version":       "1-0-0",
	"app_id":              "test-data<>",
	"dvce_created_tstamp": testLuaTimes["dvceCreatedTstamp"],
	"event":               "unstruct",
	"v_collector":         "ssc-0.15.0-googlepubsub",
	"network_userid":      "d26822f5-52cc-4292-8f77-14ef6b7a27e2",
	"event_name":          "add_to_cart",
	"event_vendor":        "com.snowplowanalytics.snowplow",
	"event_format":        "jsonschema",
	"platform":            "pc",
	"etl_tstamp":          testLuaTimes["etlTstamp"],
	"collector_tstamp":    testLuaTimes["collectorTstamp"],
	"user_id":             "user<built-in function input>",
	"dvce_sent_tstamp":    testLuaTimes["dvceSentTstamp"],
	"derived_tstamp":      testLuaTimes["derivedTstamp"],
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

var testLuaTsv = []byte(`test-data<>	pc	2019-05-10 14:40:37.436	2019-05-10 14:40:35.972	2019-05-10 14:40:35.551	unstruct	e9234345-f042-46ad-b1aa-424464066a33			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	1.2.3.4				d26822f5-52cc-4292-8f77-14ef6b7a27e2																																									{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0","data":{"sku":"item41","quantity":2,"unitPrice":32.4,"currency":"GBP"}}}																			python-requests/2.21.0																																										2019-05-10 14:40:35.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:35.972	com.snowplowanalytics.snowplow	add_to_cart	jsonschema	1-0-0		`)

// corresponding JSON to previous TSV
var testLuaJSON = []byte(`{"app_id":"test-data<>","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"1.2.3.4","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// json encoded inside Lua
var snowplowJSON1ChangedLua = []byte(`{"app_id_CHANGED":"test-data1","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user\u003cbuilt-in function input\u003e","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)
