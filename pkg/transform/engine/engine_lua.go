// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package engine

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	gojson "github.com/goccy/go-json"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/yuin/gluamapper"
	lua "github.com/yuin/gopher-lua"
	luaparse "github.com/yuin/gopher-lua/parse"
	luajson "layeh.com/gopher-json"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// luaEngineConfig configures the Lua Engine.
type luaEngineConfig struct {
	Name       string `hcl:"name"`
	SourceB64  string `hcl:"source_b64" env:"TRANSFORMATION_LUA_SOURCE_B64"`
	RunTimeout int    `hcl:"timeout_sec,optional" env:"TRANSFORMATION_LUA_TIMEOUT_SEC"`
	Sandbox    bool   `hcl:"sandbox,optional" env:"TRANSFORMATION_LUA_SANDBOX"`
	SpMode     bool   `hcl:"snowplow_mode,optional" env:"TRANSFORMATION_LUA_SNOWPLOW_MODE"`
}

// LuaEngine handles the provision of a Lua runtime to run transformations.
type LuaEngine struct {
	Name       string
	Code       *lua.FunctionProto
	RunTimeout time.Duration
	Options    *lua.Options
	SpMode     bool
}

// GetName returns the engine's name
func (e *LuaEngine) GetName() string {
	return e.Name
}

// newLuaEngine returns a Lua Engine from a luaEngineConfig.
func newLuaEngine(c *luaEngineConfig) (*LuaEngine, error) {
	luaSrc, err := base64.StdEncoding.DecodeString(c.SourceB64)
	if err != nil {
		return nil, err
	}

	compiledCode, err := compileLuaCode(string(luaSrc), c.SourceB64)
	if err != nil {
		return nil, err
	}

	eng := &LuaEngine{
		Name:       c.Name,
		Code:       compiledCode,
		RunTimeout: time.Duration(c.RunTimeout) * time.Second,
		Options:    &lua.Options{SkipOpenLibs: c.Sandbox},
		SpMode:     c.SpMode,
	}

	return eng, nil
}

// The LuaEngineAdapter type is an adapter for functions to be used as
// pluggable components for Lua Engine. It implements the Pluggable interface.
type LuaEngineAdapter func(i interface{}) (interface{}, error)

// AdaptLuaEngineFunc returns a LuaEngineAdapter.
func AdaptLuaEngineFunc(f func(c *luaEngineConfig) (*LuaEngine, error)) LuaEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*luaEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected luaEngineConfig")
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
	cfg := &luaEngineConfig{
		RunTimeout: 5,
		Sandbox:    true,
	}

	return cfg, nil
}

// LuaEngineConfigFunction returns the Pluggable transformation layer implemented in Lua.
func LuaEngineConfigFunction(t *luaEngineConfig) (*LuaEngine, error) {
	return newLuaEngine(&luaEngineConfig{
		Name:       t.Name,
		SourceB64:  t.SourceB64,
		RunTimeout: t.RunTimeout,
		Sandbox:    t.Sandbox,
		SpMode:     t.SpMode,
	})
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
			encoded, err := gojson.MarshalWithOption(protocol.Data, gojson.DisableHTMLEscape())
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

		return message, nil, nil, protocol

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
func compileLuaCode(code, name string) (*lua.FunctionProto, error) {
	reader := strings.NewReader(code)
	chunk, err := luaparse.Parse(reader, code)
	if err != nil {
		return nil, err
	}
	proto, err := lua.Compile(chunk, name)
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
		return fmt.Errorf("global main() Lua function not found: %q", funcName)
	}

	return nil
}

// mkLuaEngineInput describes the process of constructing input to Lua engine.
// No side effects.
func mkLuaEngineInput(e *LuaEngine, message *models.Message, interState interface{}) (*lua.LTable, error) {
	if interState != nil {
		if i, ok := interState.(*engineProtocol); ok {
			return toLuaTable(i)
		}
	}

	candidate := &engineProtocol{
		Data: string(message.Data),
	}

	if !e.SpMode {
		return toLuaTable(candidate)
	}

	parsedMessage, err := transform.IntermediateAsSpEnrichedParsed(interState, message)
	if err != nil {
		// if spMode, error for non Snowplow enriched event data
		return nil, err
	}

	spMap, err := parsedMessage.ToMap()
	if err != nil {
		return nil, err
	}
	candidate.Data = spMap

	return toLuaTable(candidate)
}

// toLuaTable
func toLuaTable(p *engineProtocol) (*lua.LTable, error) {
	var tmpMap map[string]interface{}

	err := mapstructure.Decode(p, &tmpMap)
	if err != nil {
		return nil, fmt.Errorf("error decoding to map")
	}

	return mapToLTable(tmpMap)
}

// mapToLTable converts a Go map to a lua table
// see: https://github.com/yuin/gopher-lua/issues/160#issuecomment-447608033
func mapToLTable(m map[string]interface{}) (*lua.LTable, error) {
	timeLayout := "2006-01-02T15:04:05.999Z07:00"

	// Main table pointer
	ltbl := &lua.LTable{}

	// Loop map
	for key, val := range m {

		switch val.(type) {
		case float64:
			ltbl.RawSetString(key, lua.LNumber(val.(float64)))
		case int64:
			ltbl.RawSetString(key, lua.LNumber(val.(int64)))
		case string:
			ltbl.RawSetString(key, lua.LString(val.(string)))
		case bool:
			ltbl.RawSetString(key, lua.LBool(val.(bool)))
		case []byte:
			ltbl.RawSetString(key, lua.LString(string(val.([]byte))))
		case map[string]interface{}:
			// Get table from map
			tmp, err := mapToLTable(val.(map[string]interface{}))
			if err != nil {
				return nil, err
			}
			ltbl.RawSetString(key, tmp)
		case time.Time:
			t := val.(time.Time).Format(timeLayout)
			ltbl.RawSetString(key, lua.LString(t))
		case []map[string]interface{}:
			// Create slice table
			sliceTable := &lua.LTable{}
			for _, vv := range val.([]map[string]interface{}) {
				next, err := mapToLTable(vv)
				if err != nil {
					return nil, err
				}
				sliceTable.Append(next)
			}
			ltbl.RawSetString(key, sliceTable)
		case []interface{}:
			// Create slice table
			sliceTable := &lua.LTable{}
			for _, vv := range val.([]interface{}) {
				switch vv.(type) {
				case map[string]interface{}:
					// Convert map to table
					m, err := mapToLTable(vv.(map[string]interface{}))
					if err != nil {
						return nil, err
					}
					sliceTable.Append(m)
				case float64:
					sliceTable.Append(lua.LNumber(vv.(float64)))
				case string:
					sliceTable.Append(lua.LString(vv.(string)))
				case bool:
					sliceTable.Append(lua.LBool(vv.(bool)))
				}
			}

			// Append to main table
			ltbl.RawSetString(key, sliceTable)
		}
	}

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
