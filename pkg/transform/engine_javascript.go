// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"encoding/base64"
	"fmt"
	"time"

	goja "github.com/dop251/goja"
	gojaparser "github.com/dop251/goja/parser"
	gojson "github.com/goccy/go-json"
	"github.com/mitchellh/mapstructure"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// jsEngineConfig configures the JavaScript Engine.
type jsEngineConfig struct {
	SourceB64         string `hcl:"source_b64" env:"TRANSFORMATION_JS_SOURCE_B64"`
	RunTimeout        int    `hcl:"timeout_sec,optional" env:"TRANSFORMATION_JS_TIMEOUT_SEC"`
	DisableSourceMaps bool   `hcl:"disable_source_maps,optional" env:"TRANSFORMATION_JS_DISABLE_SOURCE_MAPS"`
	SpMode            bool   `hcl:"snowplow_mode,optional" env:"TRANSFORMATION_JS_SNOWPLOW_MODE"`
}

// jsEngine handles the provision of a JavaScript runtime to run transformations.
type jsEngine struct {
	Code       *goja.Program
	RunTimeout time.Duration
	SpMode     bool
}

// newJSEngine returns a JavaScript Engine from a jsEngineConfig.
func newJSEngine(c *jsEngineConfig) (*jsEngine, error) {
	jsSrc, err := base64.StdEncoding.DecodeString(c.SourceB64)
	if err != nil {
		return nil, err
	}

	compiledCode, err := compileJS(string(jsSrc), c.SourceB64, c.DisableSourceMaps)
	if err != nil {
		return nil, err
	}

	eng := &jsEngine{
		Code:       compiledCode,
		RunTimeout: time.Duration(c.RunTimeout) * time.Second,
		SpMode:     c.SpMode,
	}

	return eng, nil
}

// The jsEngineAdapter type is an adapter for functions to be used as
// pluggable components for JavaScript Engine. Implements the Pluggable interface.
type jsEngineAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f jsEngineAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f jsEngineAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &jsEngineConfig{
		RunTimeout:        5,
		DisableSourceMaps: true,
	}

	return cfg, nil
}

// adaptJSEngineFunc returns a jsEngineAdapter.
func adaptJSEngineFunc(f func(c *jsEngineConfig) (*jsEngine, error)) jsEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*jsEngineConfig)
		if !ok {
			return nil, fmt.Errorf("invalid input, expected jsEngineConfig")
		}

		return f(cfg)
	}
}

// JSLayer returns the Pluggable transformation layer implemented in JavaScript.
func JSLayer() interface{} {
	return adaptJSEngineFunc(newJSEngine)
}

// SmokeTest implements SmokeTester.
func (e *jsEngine) SmokeTest(funcName string) error {
	_, _, err := initRuntime(e, funcName)
	return err
}

// MakeFunction implements FunctionMaker.
func (e *jsEngine) MakeFunction(funcName string) TransformationFunction {

	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		// making input
		input, err := mkJSEngineInput(e, message, interState)
		if err != nil {
			message.SetError(fmt.Errorf("failed making input for the JavaScript runtime: %q", err.Error()))
			return nil, nil, message, nil
		}

		// initializing
		vm, fun, err := initRuntime(e, funcName)
		if err != nil {
			message.SetError(fmt.Errorf("failed initializing JavaScript runtime: %q", err.Error()))
			return nil, nil, message, nil
		}

		timer := time.AfterFunc(e.RunTimeout, func() {
			vm.Interrupt("runtime deadline exceeded")
		})
		defer timer.Stop()

		// running
		res, err := fun(goja.Undefined(), vm.ToValue(input))

		if err != nil {
			// runtime error counts as failure
			runErr := fmt.Errorf("error running JavaScript function %q: %q", funcName, err.Error())
			message.SetError(runErr)
			return nil, nil, message, nil
		}

		// validating output
		protocol, err := validateJSEngineOut(res.Export())
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		// filtering - keeping same behaviour with spEnrichedFilter
		if protocol.FilterOut == true {
			return nil, message, nil, nil
		}

		// handling data
		switch protoData := protocol.Data.(type) {
		case string:
			message.Data = []byte(protoData)
		case map[string]interface{}:
			// encode
			encoded, err := gojson.MarshalWithOption(protoData, gojson.DisableHTMLEscape())
			if err != nil {
				message.SetError(fmt.Errorf("error encoding message data"))
				return nil, nil, message, nil
			}
			message.Data = encoded
		default:
			message.SetError(fmt.Errorf("invalid return type from JavaScript transformation; expected string or object"))
			return nil, nil, message, nil
		}

		// setting pk if needed
		pk := protocol.PartitionKey
		if pk != "" && message.PartitionKey != pk {
			message.PartitionKey = pk
		}

		return message, nil, nil, protocol
	}
}

// compileJS compiles JavaScript code.
// Since goja.New is not goroutine-safe, we spin a new runtime for every
// transformation. The reason for this function is to allow us to at least share
// the compiled code and so run only once the parse and compile steps,
// which are implicitly run by the alternative RunString.
// see also:
// https://pkg.go.dev/github.com/dop251/goja#CompileAST
func compileJS(code, name string, disableSrcMaps bool) (*goja.Program, error) {
	parserOpts := make([]gojaparser.Option, 0, 1)

	if disableSrcMaps == true {
		parserOpts = append(parserOpts, gojaparser.WithDisableSourceMaps)
	}

	ast, err := goja.Parse(name, code, parserOpts...)
	if err != nil {
		return nil, err
	}

	// 'use strict'
	prog, err := goja.CompileAST(ast, true)
	if err != nil {
		return nil, err
	}

	return prog, nil
}

// initRuntime initializes and returns an instance of a JavaScript runtime.
func initRuntime(e *jsEngine, funcName string) (*goja.Runtime, goja.Callable, error) {
	// goja.New returns *goja.Runtime
	vm := goja.New()
	timer := time.AfterFunc(e.RunTimeout, func() {
		vm.Interrupt("runtime deadline exceeded")
	})
	defer timer.Stop()

	_, err := vm.RunProgram(e.Code)
	if err != nil {
		return nil, nil, fmt.Errorf("could not load JavaScript code: %q", err)
	}

	if fun, ok := goja.AssertFunction(vm.Get(funcName)); ok {
		return vm, fun, nil
	}

	return nil, nil, fmt.Errorf("could not assert as function: %q", funcName)
}

// mkJSEngineInput describes the logic for constructing the input to JS engine.
// No side effects.
func mkJSEngineInput(e *jsEngine, message *models.Message, interState interface{}) (*EngineProtocol, error) {
	if interState != nil {
		if i, ok := interState.(*EngineProtocol); ok {
			return i, nil
		}
	}

	candidate := &EngineProtocol{
		Data: string(message.Data),
	}

	if !e.SpMode {
		return candidate, nil
	}

	parsedMessage, err := intermediateAsSpEnrichedParsed(interState, message)
	if err != nil {
		// if spMode, error for non Snowplow enriched event data
		return nil, err
	}

	spMap, err := parsedMessage.ToMap()
	if err != nil {
		return nil, err
	}

	candidate.Data = spMap
	return candidate, nil
}

// validateJSEngineOut validates the value returned by the js engine.
func validateJSEngineOut(output interface{}) (*EngineProtocol, error) {
	if output == nil {
		return nil, fmt.Errorf("invalid return type from JavaScript transformation; got null or undefined")
	}

	if out, ok := output.(*EngineProtocol); ok {
		return out, nil
	}

	outMap, ok := output.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid return type from JavaScript transformation")
	}

	result := &EngineProtocol{}
	err := mapstructure.Decode(outMap, result)
	if err != nil {
		return nil, fmt.Errorf("protocol violation in return value from JavaScript transformation")
	}

	return result, nil
}
