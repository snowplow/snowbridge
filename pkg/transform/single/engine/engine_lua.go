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

package engine

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/yuin/gluamapper"
	lua "github.com/yuin/gopher-lua"
	luaparse "github.com/yuin/gopher-lua/parse"
	luajson "layeh.com/gopher-json"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	transform "github.com/snowplow/snowbridge/pkg/transform/single"
)

// LuaEngineConfig configures the Lua Engine.
type LuaEngineConfig struct {
	ScriptPath string `hcl:"script_path,optional"`
	RunTimeout int    `hcl:"timeout_sec,optional"`
	Sandbox    bool   `hcl:"sandbox,optional"`
}

// LuaEngine handles the provision of a Lua runtime to run transformations.
type LuaEngine struct {
	Code       *lua.FunctionProto
	RunTimeout time.Duration
	Options    *lua.Options
}

// NewLuaEngine returns a Lua Engine from a LuaEngineConfig.
func NewLuaEngine(c *LuaEngineConfig, script string) (*LuaEngine, error) {
	compiledCode, err := compileLuaCode(script)
	if err != nil {
		return nil, err
	}

	eng := &LuaEngine{
		Code:       compiledCode,
		RunTimeout: time.Duration(c.RunTimeout) * time.Second,
		Options:    &lua.Options{SkipOpenLibs: c.Sandbox},
	}

	return eng, nil
}

// The LuaEngineAdapter type is an adapter for functions to be used as
// pluggable components for Lua transformation. It implements the Pluggable interface.
type LuaEngineAdapter func(i interface{}) (interface{}, error)

// AdaptLuaEngineFunc returns a LuaEngineAdapter.
func AdaptLuaEngineFunc(f func(c *LuaEngineConfig) (*LuaEngine, error)) LuaEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*LuaEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected LuaEngineConfig")
		}

		return f(cfg)
	}
}

// Create implements the ComponentCreator interface.
func (f LuaEngineAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f LuaEngineAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &LuaEngineConfig{
		RunTimeout: 5,
		Sandbox:    true,
	}

	return cfg, nil
}

// LuaAdapterGenerator returns a lua transformation adapter.
func LuaAdapterGenerator(f func(c *LuaEngineConfig) (transform.TransformationFunction, error)) LuaEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*LuaEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// LuaConfigFunction returns a lua transformation function, from a LuaEngineConfig.
func LuaConfigFunction(c *LuaEngineConfig) (transform.TransformationFunction, error) {
	script, err := os.ReadFile(c.ScriptPath)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error reading script at path %s", c.ScriptPath))
	}

	engine, err := NewLuaEngine(c, string(script))
	if err != nil {
		return nil, errors.Wrap(err, "error building Lua engine")
	}

	smkTestErr := engine.SmokeTest("main")
	if smkTestErr != nil {
		return nil, errors.Wrap(smkTestErr, "error smoke testing Lua function")
	}

	return engine.MakeFunction("main"), nil
}

// LuaConfigPair is a configuration pair for the lua transformation
var LuaConfigPair = config.ConfigurationPair{
	Name:   "lua",
	Handle: LuaAdapterGenerator(LuaConfigFunction),
}

// SmokeTest implements smokeTester.
func (e *LuaEngine) SmokeTest(funcName string) error {
	// setup the Lua state
	L := lua.NewState(*e.Options) // L is ptr
	defer L.Close()

	d := time.Now().Add(e.RunTimeout)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	L.SetContext(ctx)

	return initVM(e, L, funcName)
}

// MakeFunction implements functionMaker.
func (e *LuaEngine) MakeFunction(funcName string) transform.TransformationFunction {

	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// making input
		input, err := mkLuaEngineInput(e, message, interState)
		if err != nil {
			message.SetError(fmt.Errorf("failed making input for the Lua runtime: %q", err.Error()))
			return nil, nil, message, nil
		}

		// setup the Lua state
		L := lua.NewState(*e.Options)
		defer L.Close()

		d := time.Now().Add(e.RunTimeout)
		ctx, cancel := context.WithDeadline(context.Background(), d)
		defer cancel()
		L.SetContext(ctx)

		err = initVM(e, L, funcName)
		if err != nil {
			message.SetError(fmt.Errorf("failed initializing Lua runtime: %q", err.Error()))
			return nil, nil, message, nil
		}

		// running
		err = L.CallByParam(lua.P{
			Fn:      L.GetGlobal(funcName), // name of Lua function
			NRet:    1,                     // num of return values
			Protect: true,                  // don't panic
		}, input)
		if err != nil {
			// runtime error counts as failure
			runErr := fmt.Errorf("error running Lua function %q: %q", funcName, err.Error())
			message.SetError(runErr)
			return nil, nil, message, nil
		}

		// validating output
		protocol, err := validateLuaEngineOut(L.Get(-1))
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		// filtering - keeping same behaviour with spEnrichedFilter
		if protocol.FilterOut == true {
			return nil, message, nil, nil
		}

		// handling data
		encode := false
		switch protoData := protocol.Data.(type) {
		case string:
			message.Data = []byte(protoData)
		case map[string]interface{}:
			encode = true
		case map[interface{}]interface{}:
			encode = true
			siData := toStringIfaceMap(protoData)
			protocol.Data = siData
		default:
			message.SetError(fmt.Errorf("invalid return type from Lua transformation; expected string or table"))
			return nil, nil, message, nil
		}

		// encode
		if encode {
			encoded, err := json.Marshal(protocol.Data)
			if err != nil {
				message.SetError(fmt.Errorf("error encoding message data"))
				return nil, nil, message, nil
			}
			message.Data = encoded
		}

		// setting pk if needed
		pk := protocol.PartitionKey
		if pk != "" && message.PartitionKey != pk {
			message.PartitionKey = pk
		}

		return message, nil, nil, nil

	}
}

// compileLuaCode compiles lua code.
// Since lua.NewState is not goroutine-safe, we spin a new state for every
// transformation. The reason for this function is to allow us to at least share
// the compiled bytecode (which is read-only and thus safe) and so run only once
// the load, parse and compile steps, which are implicitly run by the alternative
// lua.DoString.
// see also:
// https://github.com/yuin/gopher-lua/pull/193
// https://github.com/yuin/gopher-lua#sharing-lua-byte-code-between-lstates
func compileLuaCode(code string) (*lua.FunctionProto, error) {
	reader := strings.NewReader(code)
	chunk, err := luaparse.Parse(reader, code)
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, "main")
	if err != nil {
		return nil, err
	}
	return proto, nil
}

// loadLuaCode loads compiled Lua code into a lua state
func loadLuaCode(ls *lua.LState, proto *lua.FunctionProto) error {
	lfunc := ls.NewFunctionFromProto(proto)
	ls.Push(lfunc)

	// https://github.com/yuin/gopher-lua/blob/f4c35e4016d9d8580b007ebaeb68ecd8e0b09f1c/_state.go#L1811
	return ls.PCall(0, lua.MultRet, nil)
}

// initVM performs the initialization steps for a Lua state.
func initVM(e *LuaEngine, L *lua.LState, funcName string) error {
	if e.Options.SkipOpenLibs == false {
		luajson.Preload(L)
	}

	err := loadLuaCode(L, e.Code)
	if err != nil {
		return fmt.Errorf("could not load lua code: %q", err)
	}

	if _, ok := L.GetGlobal(funcName).(*lua.LFunction); !ok {
		return fmt.Errorf("global Lua function not found: %q", funcName)
	}

	return nil
}

// mkLuaEngineInput describes the process of constructing input to Lua engine.
// No side effects.
func mkLuaEngineInput(e *LuaEngine, message *models.Message, interState interface{}) (*lua.LTable, error) {
	ltbl := &lua.LTable{}

	ltbl.RawSetString("Data", lua.LString(string(message.Data)))
	ltbl.RawSetString("PartitionKey", lua.LString(message.PartitionKey))
	ltbl.RawSetString("FilterOut", lua.LBool(false))

	return ltbl, nil
}

// validateLuaEngineOut validates the value returned from the Lua engine is a
// Lua Table (lua.LTable) and that it maps to engineProtocol.
func validateLuaEngineOut(output interface{}) (*engineProtocol, error) {
	if output == nil {
		return nil, fmt.Errorf("invalid return type from Lua transformation; got nil")
	}

	if luaTablePtr, ok := output.(*lua.LTable); ok {
		result := &engineProtocol{}
		luaMapper := gluamapper.NewMapper(gluamapper.Option{
			NameFunc: gluamapper.Id,
		})

		err := luaMapper.Map(luaTablePtr, result)
		if err != nil {
			return nil, fmt.Errorf("protocol violation in return value from Lua transformation")
		}

		return result, nil
	}

	return nil, fmt.Errorf("invalid return type from Lua transformation; expected Lua Table")
}

// toStringIfaceMap converts map[interface{}]interface{} to map[string]interface.
// This function is used in Lua Engine because of how gluamapper actually maps
// lua.LTable to Go map.
// see:https://github.com/yuin/gluamapper/blob/d836955830e75240d46ce9f0e6d148d94f2e1d3a/gluamapper.go#L44
func toStringIfaceMap(interfaceMap map[interface{}]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for key, val := range interfaceMap {
		result[fmt.Sprintf("%v", key)] = doValue(val)
	}

	return result
}

// doValue is a helper for toStringIfaceMap, to cover for values that are
// []interface{} and map[interface{}]interface.
func doValue(value interface{}) interface{} {
	switch value := value.(type) {
	case []interface{}:
		return doIfaceSlice(value)
	case map[interface{}]interface{}:
		return toStringIfaceMap(value)
	default:
		return value
	}
}

// doIfaceSlice is a helper for doValue to handle interface slices.
func doIfaceSlice(iSlice []interface{}) []interface{} {
	result := make([]interface{}, len(iSlice))
	for i, val := range iSlice {
		result[i] = doValue(val)
	}

	return result
}
