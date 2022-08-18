// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package engine

import (
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

func TestLuaLayer(t *testing.T) {
	assert := assert.New(t)
	layer, err := LuaEngineConfigFunction(&LuaEngineConfig{
		SourceB64:  "CglmdW5jdGlvbiBmb28oeCkKICAgICAgICAgICByZXR1cm4geAogICAgICAgIGVuZAoJ",
		RunTimeout: 5,
		Sandbox:    false,
		SpMode:     false,
	})
	assert.Nil(err)
	assert.NotNil(layer)
}

func TestLuaEngineMakeFunction_SpModeFalse_IntermediateNil(t *testing.T) {
	var testInterState interface{} = nil
	var testSpMode bool = false
	testCases := []struct {
		Src           string
		Scenario      string
		Sandbox       bool
		Input         *models.Message
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Src: `
function main(x)
  return x
end
`,
			Scenario: "main",
			Sandbox:  true,
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
function main(x)
  x.Data = "Hello:" .. x.Data
  return x
end
`,
			Scenario: "main",
			Sandbox:  true,
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
function main(x)
  x.FilterOut = false
  return x
end
`,
			Scenario: "main",
			Sandbox:  true,
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
function main(x)
  if type(x.Data) == "string" then
     return { FilterOut = true }
  end
  return { FilterOut = false }
end
`,
			Scenario: "main",
			Sandbox:  false,
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

function main(x)
  local dat = x["Data"]
  local jsonObj, decodeErr = json.decode(dat)
  if decodeErr then error(decodeErr) end

  local result, encodeErr = json.encode(jsonObj)
  if encodeErr then error(encodeErr) end

  x.Data = result
  return x
end
`,
			Scenario: "main",
			Sandbox:  false,
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(snowplowJSON1),
			},
			Error: nil,
		},
		{
			Src: `
local json = require("json")

function main(x)
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
			Scenario: "main",
			Sandbox:  false,
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(snowplowJSON1ChangedLua),
			},
			Error: nil,
		},
		{
			Src: `
local json = require("json")

function main(x)
  local jsonObj, decodeErr = json.decode(x["Data"])
  if decodeErr then error(decodeErr) end

  if jsonObj["app_id"] == "filterMeOut" then
     return { FilterOut = false, Data = x["Data"] }
  else
     return { FilterOut = true }
  end
end
`,
			Scenario: "main",
			Sandbox:  false,
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
function main(x)
  return 0
end
`,
			Scenario: "main",
			Sandbox:  true,
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
function main(x)
end
`,
			Scenario: "main",
			Sandbox:  true,
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
function main(x)
  return nil
end
`,
			Scenario: "main",
			Sandbox:  true,
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
function main(x)
  return 2 * x
end
`,
			Scenario: "main",
			Sandbox:  true,
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
			Error:         fmt.Errorf("error running Lua function \"main\""),
		},
		{
			Src: `
function main(x)
  error("Failed")
end
`,
			Scenario: "main",
			Sandbox:  false,
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
			Error:         fmt.Errorf("error running Lua function \"main\""),
		},
		{
			Src: `
local clock = os.clock

function main(x)
  local t0 = clock()
  while clock() - t0 <= 10 do end
end
`,
			Scenario: "main",
			Sandbox:  false,
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
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			src := base64.StdEncoding.EncodeToString([]byte(tt.Src))
			luaConfig := &LuaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := NewLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(tt.Scenario); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(tt.Scenario)
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
		Sandbox       bool
		Input         *models.Message
		Expected      map[string]*models.Message
		ExpInterState interface{}
		Error         error
	}{
		{
			Scenario: "main",
			Src: `
function main(x)
  return x
end
`,
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "filtering",
			Src: `
function main(input)
  -- input is a lua table
  local spData = input["Data"]
  if spData["app_id"] == "myApp" then
     return input;
  end
  return { FilterOut = true }
end
`,
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
function main(x)
  local ret = {
     FilterOut = true,
     Data = "shouldNotAppear",
     PartitionKey = "notThis"
  }
  return ret
end
`,
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
function main(x)
  return x
end
`,
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
function main(x)
  return 0
end
`,
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
			luaConfig := &LuaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := NewLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(`main`); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(`main`)
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
function main(x)
  return x
end
`,
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function main(x)
  return x
end
`,
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_nonSpEnriched",
			Src: `
function main(x)
   return x;
end
`,
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_not_EngineProtocol_SpEnriched",
			Src: `
function main(x)
   return x;
end
`,
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
			ExpInterState: &engineProtocol{
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
			luaConfig := &LuaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := NewLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(`main`); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(`main`)
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
function main(x)
  return x
end
`,
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_EngineProtocol_String",
			Src: `
function main(x)
  return x
end
`,
			Sandbox: true,
			Input: &models.Message{
				Data:         testLuaJSON,
				PartitionKey: "some-test-key",
			},
			InterState: &engineProtocol{
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "",
				Data:         string(testLuaJSON),
			},
			Error: nil,
		},
		{
			Scenario: "intermediateState_notEngineProtocol_notSpEnriched",
			Src: `
function main(x)
  return x
end
`,
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
function main(x)
  return x
end
`,
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
			ExpInterState: &engineProtocol{
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
			luaConfig := &LuaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     testSpMode,
			}

			luaEngine, err := NewLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(`main`); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(`main`)
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
function main(x)
   x["PartitionKey"] = "newPk"
   return x
end
`,
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         testLuaMap,
			},
			Error: nil,
		},
		{
			Scenario: "onlySetPk_spModeFalse",
			Src: `
function main(x)
   x["PartitionKey"] = "newPk"
   return x
end
`,
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
			ExpInterState: &engineProtocol{
				FilterOut:    false,
				PartitionKey: "newPk",
				Data:         string(testLuaTsv),
			},
			Error: nil,
		},
		{
			Scenario: "filterOutIgnores",
			Src: `
function main(x)
  local ret = {
     FilterOut = true,
     Data = "shouldNotAppear",
     PartitionKey = "notThis"
  }
  return ret
end
`,
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
			luaConfig := &LuaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
				SpMode:     tt.SpMode,
			}

			luaEngine, err := NewLuaEngine(luaConfig)
			assert.NotNil(luaEngine)
			if err != nil {
				t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
			}

			if err := luaEngine.SmokeTest(`main`); err != nil {
				t.Fatalf("smoke-test failed with error: %q", err.Error())
			}

			transFunction := luaEngine.MakeFunction(`main`)
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
function main(x)
  return x
end
`,
			FunName:      "main",
			Sandbox:      true,
			CompileError: nil,
			SmokeError:   nil,
		},
		{
			Src: `
function wrong_name(x)
  return "something"
end
`,
			FunName:      "main",
			Sandbox:      true,
			CompileError: nil,
			SmokeError:   fmt.Errorf("global Lua function not found: \"main\""),
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
function main(x)
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
			luaConfig := &LuaEngineConfig{
				SourceB64:  src,
				RunTimeout: 1,
				Sandbox:    tt.Sandbox,
			}

			luaEngine, compileErr := NewLuaEngine(luaConfig)

			if compileErr != nil {
				if tt.CompileError == nil {
					t.Fatalf("got unexpected error while creating NewLuaEngine: %s", compileErr.Error())
				}

				if !strings.Contains(compileErr.Error(), tt.CompileError.Error()) {
					t.Errorf("NewLuaEngine error mismatch\nGOT_ERROR:\n%q\n does not contain\nEXPECTED_ERROR:\n%q",
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
function main(x)
  return x
end
`
	funcname := "main"
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	if err != nil {
		t.Fatalf("NewLuaEngine failed with error: %q", err)
	}

	if err := luaEngine.SmokeTest(funcname); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	luaFunc := luaEngine.MakeFunction(funcname)
	setPkToAppID := transform.NewSpEnrichedSetPkFunction("app_id")
	spEnrichedToJSON := transform.SpEnrichedToJSON

	testCases := []struct {
		Name           string
		Transformation transform.TransformationApplyFunction
	}{
		{
			Transformation: transform.NewTransformation(
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
				assert.JSONEq(string(res.Data), string(exp.Data))
				assert.Equal(res.PartitionKey, exp.PartitionKey)

			}
		})
	}

}

func TestLuaEngineWithBuiltinsSpModeFalse(t *testing.T) {
	srcCode := `
function main(x)
  return x
end

function setPk(x)
  x["PartitionKey"] = "testKey"
  return x
end
`
	// Lua
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
		SpMode:     false,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	if err != nil {
		t.Fatalf("NewLuaEngine failed with error: %q", err)
	}

	if err := luaEngine.SmokeTest("main"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	luaFuncID := luaEngine.MakeFunction("main")
	luaFuncPk := luaEngine.MakeFunction("setPk")

	// Builtins
	setPkToAppID := transform.NewSpEnrichedSetPkFunction("app_id")
	spEnrichedToJSON := transform.SpEnrichedToJSON

	testCases := []struct {
		Name           string
		Transformation transform.TransformationApplyFunction
		Input          []*models.Message
		ExpectedGood   []*models.Message
	}{
		{
			Input: messages,
			Transformation: transform.NewTransformation(
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
			Input: messages,
			Transformation: transform.NewTransformation(
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
			Input: messages,
			Transformation: transform.NewTransformation(
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
					assert.JSONEq(string(res.Data), string(exp.Data))
					assert.Equal(res.PartitionKey, exp.PartitionKey)
				}
			}
		})
	}
}

func TestLuaEngineWithBuiltinsSpModeTrue(t *testing.T) {
	srcCode := `
function main(x)
  return x
end

function setPk(x)
  x["PartitionKey"] = "testKey"
  return x
end
`
	// Lua
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
		SpMode:     true,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	if err != nil {
		t.Fatalf("NewLuaEngine failed with error: %q", err)
	}

	if err := luaEngine.SmokeTest("main"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}
	if err := luaEngine.SmokeTest("setPk"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	luaFuncID := luaEngine.MakeFunction("main")
	luaFuncPk := luaEngine.MakeFunction("setPk")

	// Builtins
	setPkToAppID := transform.NewSpEnrichedSetPkFunction("app_id")
	spEnrichedToJSON := transform.SpEnrichedToJSON

	testCases := []struct {
		Name           string
		Transformation transform.TransformationApplyFunction
		Input          []*models.Message
		ExpectedGood   []*models.Message
	}{
		{
			Input: []*models.Message{
				{
					Data:         testLuaTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: transform.NewTransformation(
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
			Input: []*models.Message{
				{
					Data:         testLuaTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: transform.NewTransformation(
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
			Input: []*models.Message{
				{
					Data:         testLuaTsv,
					PartitionKey: "prevKey",
				},
			},
			Transformation: transform.NewTransformation(
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
					assert.JSONEq(string(res.Data), string(exp.Data))
					assert.Equal(res.PartitionKey, exp.PartitionKey)
				}
			}
		})
	}
}

func Benchmark_LuaEngine_Passthrough_Sandboxed(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x)
  return x
end
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))

	inputMsg := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-test-key",
	}
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    true,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	if err != nil {
		b.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("main")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_LuaEngine_Passthrough(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x)
  return x
end
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))

	inputMsg := &models.Message{
		Data:         snowplowJSON1,
		PartitionKey: "some-test-key",
	}
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    false,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	if err != nil {
		b.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("main")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
}

func Benchmark_LuaEngine_Passthrough_Json(b *testing.B) {
	b.ReportAllocs()

	srcCode := `
function main(x)
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
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    false,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	if err != nil {
		b.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("jsonIdentity")

	for n := 0; n < b.N; n++ {
		transFunction(inputMsg, nil)
	}
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

func TestLuaQuick(t *testing.T) {
	assert := assert.New(t)

	script := `function main(input)
	-- input is a lua table
	local spData = input["Data"]
	if spData["platform"] ~= "web" then
	   return { FilterOut = true };
	end

	if spData["user_id"] ~= nil then
		spData["uid"] = spData["user_id"]
	else
		spData["uid"] = spData["domain_userid"]
	end
	return  { Data = spData, PartitionKey = app_id }
  end`

	src := base64.StdEncoding.EncodeToString([]byte(script))
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 10,
		Sandbox:    false,
		SpMode:     true,
	}

	luaEngine, err := NewLuaEngine(luaConfig)
	assert.NotNil(luaEngine)
	if err != nil {
		t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("main")
	s, f, e, _ := transFunction(&models.Message{
		Data:         testJsTsv2, //         []byte(`{"name": "Bruce", "id": "b47m4n", "batmobileCount": 1}`),
		PartitionKey: "oldPk",
	}, nil)

	assert.NotNil(s)
	assert.Nil(f)

	// assert.Nil(s)
	// assert.NotNil(f)

	if e != nil {
		fmt.Println(e.GetError())
	}

	fmt.Println(s)
}
