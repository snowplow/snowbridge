// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package engine

import (
	"fmt"
	"os"
	"time"

	goja "github.com/dop251/goja"
	gojaparser "github.com/dop251/goja/parser"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// JSEngineConfig configures the JavaScript Engine.
type JSEngineConfig struct {
	ScriptPath string `hcl:"script_path,optional"`
	RunTimeout int    `hcl:"timeout_sec,optional"`
	SpMode     bool   `hcl:"snowplow_mode,optional"`
}

// JSEngine handles the provision of a JavaScript runtime to run transformations.
type JSEngine struct {
	Code       *goja.Program
	RunTimeout time.Duration
	SpMode     bool
}

// The JSEngineAdapter type is an adapter for functions to be used as
// pluggable components for a JS transformation. It implements the Pluggable interface.
type JSEngineAdapter func(i interface{}) (interface{}, error)

// ProvideDefault returns a JSEngineConfig with default configuration values
func (f JSEngineAdapter) ProvideDefault() (interface{}, error) {
	return &JSEngineConfig{
		RunTimeout: 15,
	}, nil
}

// Create implements the ComponentCreator interface.
func (f JSEngineAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// JSAdapterGenerator returns a js transformation adapter.
func JSAdapterGenerator(f func(c *JSEngineConfig) (transform.TransformationFunction, error)) JSEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*JSEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected spEnrichedFilterConfig")
		}

		return f(cfg)
	}
}

// JSConfigFunction returns a js transformation function, from a JSEngineConfig.
func JSConfigFunction(c *JSEngineConfig) (transform.TransformationFunction, error) {
	script, err := os.ReadFile(c.ScriptPath)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("Error reading script at path %s", c.ScriptPath))
	}

	engine, err := NewJSEngine(c, string(script))
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
	return func(i interface{}) (interface{}, error) {
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
		Code:       compiledCode,
		RunTimeout: time.Duration(c.RunTimeout) * time.Second,
		SpMode:     c.SpMode,
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
			encoded, err := json.Marshal(protoData)
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
func mkJSEngineInput(e *JSEngine, message *models.Message, interState interface{}) (*engineProtocol, error) {
	if interState != nil {
		if i, ok := interState.(*engineProtocol); ok {
			return i, nil
		}
	}

	candidate := &engineProtocol{
		Data: string(message.Data),
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
func validateJSEngineOut(output interface{}) (*engineProtocol, error) {
	if output == nil {
		return nil, fmt.Errorf("invalid return type from JavaScript transformation; got null or undefined")
	}

	if out, ok := output.(*engineProtocol); ok {
		return out, nil
	}

	outMap, ok := output.(map[string]interface{})
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
