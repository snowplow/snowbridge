/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package engine

import (
	"fmt"
	"os"
	"time"

	goja "github.com/dop251/goja"
	gojaparser "github.com/dop251/goja/parser"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
)

// JSEngineConfig configures the JavaScript Engine.
type JSEngineConfig struct {
	ScriptPath     string `hcl:"script_path,optional"`
	Script         string `hcl:"script,optional"`
	RunTimeout     int    `hcl:"timeout_sec,optional"`
	SpMode         bool   `hcl:"snowplow_mode,optional"`
	RemoveNulls    bool   `hcl:"remove_nulls,optional"`
	HashSaltSecret string `hcl:"hash_salt_secret,optional"`
}

// JSEngine handles the provision of a JavaScript runtime to run transformations.
type JSEngine struct {
	Code           *goja.Program
	RunTimeout     time.Duration
	SpMode         bool
	RemoveNulls    bool
	HashSaltSecret string
}

// The JSEngineAdapter type is an adapter for functions to be used as
// pluggable components for a JS transformation. It implements the Pluggable interface.
type JSEngineAdapter func(i any) (any, error)

// ProvideDefault returns a JSEngineConfig with default configuration values
func (f JSEngineAdapter) ProvideDefault() (any, error) {
	return &JSEngineConfig{
		RunTimeout:  15,
		RemoveNulls: false,
	}, nil
}

// Create implements the ComponentCreator interface.
func (f JSEngineAdapter) Create(i any) (any, error) {
	return f(i)
}

// JSAdapterGenerator returns a js transformation adapter.
func JSAdapterGenerator(f func(c *JSEngineConfig) (transform.TransformationFunction, error)) JSEngineAdapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*JSEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// JSConfigFunction returns a js transformation function, from a JSEngineConfig.
func JSConfigFunction(c *JSEngineConfig) (transform.TransformationFunction, error) {
	var script string

	// If we have a script path, use that
	if c.ScriptPath != "" {
		scriptBytes, err := os.ReadFile(c.ScriptPath)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("Error reading script at path %s", c.ScriptPath))
		}
		script = string(scriptBytes)
	} else if c.Script != "" {
		script = c.Script
	} else {
		return nil, errors.New("JS transformation: Either script_path or script must be configured")
	}

	engine, err := NewJSEngine(c, script)
	if err != nil {
		return nil, errors.Wrap(err, "error building JS engine")
	}

	smkTestErr := engine.SmokeTest("main")
	if smkTestErr != nil {
		return nil, errors.Wrap(smkTestErr, "error smoke testing JS function")
	}

	return engine.MakeFunction("main"), nil
}

// JSConfigPair is a configuration pair for the js transformation
var JSConfigPair = config.ConfigurationPair{
	Name:   "js",
	Handle: JSAdapterGenerator(JSConfigFunction),
}

// AdaptJSEngineFunc returns an JSEngineAdapter.
func AdaptJSEngineFunc(f func(c *JSEngineConfig) (*JSEngine, error)) JSEngineAdapter {
	return func(i any) (any, error) {
		cfg, ok := i.(*JSEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected JSEngineConfig")
		}

		return f(cfg)
	}
}

// NewJSEngine returns a JSEngine from a JSEngineConfig.
func NewJSEngine(c *JSEngineConfig, script string) (*JSEngine, error) {
	compiledCode, err := compileJS(script)
	if err != nil {
		return nil, err
	}

	eng := &JSEngine{
		Code:           compiledCode,
		RunTimeout:     time.Duration(c.RunTimeout) * time.Second,
		SpMode:         c.SpMode,
		RemoveNulls:    c.RemoveNulls,
		HashSaltSecret: c.HashSaltSecret,
	}

	return eng, nil
}

// SmokeTest implements smokeTester.
func (e *JSEngine) SmokeTest(funcName string) error {
	_, _, err := initRuntime(e, funcName)
	return err
}

// MakeFunction implements functionMaker.
func (e *JSEngine) MakeFunction(funcName string) transform.TransformationFunction {

	return func(message *models.Message, interState any) (*models.Message, *models.Message, *models.Message, any) {
		// making input
		input, err := mkJSEngineInput(e, message, interState)
		if err != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "failed making input for the JavaScript runtime",
				Err:         fmt.Errorf("failed making input for the JavaScript runtime: %q", err.Error()),
			})
			return nil, nil, message, nil
		}

		// initializing
		vm, fun, err := initRuntime(e, funcName)
		if err != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "failed initializing JavaScript runtime",
				Err:         fmt.Errorf("failed initializing JavaScript runtime: %q", err.Error()),
			})
			return nil, nil, message, nil
		}

		timer := time.AfterFunc(e.RunTimeout, func() {
			vm.Interrupt("runtime deadline exceeded")
		})
		defer timer.Stop()

		// handle custom functions
		if err := vm.Set("hash", resolveHash(vm, e.HashSaltSecret)); err != nil {
			// runtime error counts as failure
			runErr := fmt.Errorf("error setting JavaScript function [%s]: %q", "hash", err.Error())
			message.SetError(runErr)
			message.SetError(&models.TransformationError{
				SafeMessage: "error setting JavaScript function [hash]",
				Err:         runErr,
			})
			return nil, nil, message, nil
		}

		// running
		res, err := fun(goja.Undefined(), vm.ToValue(input))
		if err != nil {
			// runtime error counts as failure
			message.SetError(&models.TransformationError{
				SafeMessage: fmt.Sprintf("error running JavaScript function [%s]", funcName),
				Err:         fmt.Errorf("error running JavaScript function [%s]: %q", funcName, err.Error()),
			})
			return nil, nil, message, nil
		}

		// validating output
		protocol, err := validateJSEngineOut(res.Export())
		if err != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: err.Error(),
				Err:         err,
			})
			return nil, nil, message, nil
		}

		// filtering - keeping same behaviour with spEnrichedFilter
		if protocol.FilterOut {
			return nil, message, nil, nil
		}

		// handling data
		switch protoData := protocol.Data.(type) {
		case string:
			message.Data = []byte(protoData)
		case map[string]any:

			if e.RemoveNulls {
				transform.RemoveNullFields(protoData)
			}
			// encode
			encoded, err := json.Marshal(protoData)
			if err != nil {
				message.SetError(&models.TransformationError{
					SafeMessage: "error encoding message data",
					Err:         fmt.Errorf("error encoding message data: %w", err),
				})
				return nil, nil, message, nil
			}
			message.Data = encoded
		default:
			err := fmt.Errorf("invalid return type from JavaScript transformation; expected string or object")
			message.SetError(&models.TransformationError{
				SafeMessage: err.Error(),
				Err:         err,
			})
			return nil, nil, message, nil
		}

		// setting pk if needed
		pk := protocol.PartitionKey
		if pk != "" && message.PartitionKey != pk {
			message.PartitionKey = pk
		}

		// HTTPHeaders
		if len(protocol.HTTPHeaders) > 0 {
			message.HTTPHeaders = protocol.HTTPHeaders
		}

		return message, nil, nil, protocol
	}
}

// we must be capturing the Runtime instance here, so we can handle function returns
func resolveHash(vm *goja.Runtime, hashSalt string) func(call goja.FunctionCall) goja.Value {
	return func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 2 {
			return vm.ToValue("hash() function expects 3 arguments: data and hash_func_name")
		}

		input := call.Arguments[0].String()
		hashFunctionName := call.Arguments[1].String()

		result, err := transform.DoHashing(input, hashFunctionName, hashSalt)
		if err != nil {
			vm.ToValue("")
		}
		return vm.ToValue(result)
	}
}

// compileJS compiles JavaScript code.
// Since goja.New is not goroutine-safe, we spin a new runtime for every
// transformation. The reason for this function is to allow us to at least share
// the compiled code and so run only once the parse and compile steps,
// which are implicitly run by the alternative RunString.
// see also:
// https://pkg.go.dev/github.com/dop251/goja#CompileAST
func compileJS(code string) (*goja.Program, error) {
	parserOpts := make([]gojaparser.Option, 0, 1)
	parserOpts = append(parserOpts, gojaparser.WithDisableSourceMaps)

	ast, err := goja.Parse("main", code, parserOpts...)
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
func initRuntime(e *JSEngine, funcName string) (*goja.Runtime, goja.Callable, error) {
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
func mkJSEngineInput(e *JSEngine, message *models.Message, interState any) (*engineProtocol, error) {
	if interState != nil {
		if i, ok := interState.(*engineProtocol); ok {
			return i, nil
		}
	}

	candidate := &engineProtocol{
		Data:        string(message.Data),
		HTTPHeaders: message.HTTPHeaders,
	}

	if !e.SpMode {
		return candidate, nil
	}

	parsedEvent, err := transform.IntermediateAsSpEnrichedParsed(interState, message)
	if err != nil {
		// if spMode, error for non Snowplow enriched event data
		return nil, err
	}

	spMap, err := parsedEvent.ToMap()
	if err != nil {
		return nil, err
	}

	candidate.Data = spMap
	return candidate, nil
}

// validateJSEngineOut validates the value returned by the js engine.
func validateJSEngineOut(output any) (*engineProtocol, error) {
	if output == nil {
		return nil, fmt.Errorf("invalid return type from JavaScript transformation; got null or undefined")
	}

	if out, ok := output.(*engineProtocol); ok {
		return out, nil
	}

	outMap, ok := output.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid return type from JavaScript transformation")
	}

	result := &engineProtocol{}
	err := mapstructure.Decode(outMap, result)
	if err != nil {
		return nil, fmt.Errorf("protocol violation in return value from JavaScript transformation")
	}

	return result, nil
}
