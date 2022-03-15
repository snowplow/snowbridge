// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"errors"
	"os"

	"github.com/caarlos0/env/v6"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/function"
)

// Decoder is the interface that wraps the Decode method.
type Decoder interface {
	// Decode decodes onto target given DecoderOptions.
	// The target argument must be a pointer to an allocated structure.
	Decode(opts *DecoderOptions, target interface{}) error
}

// DecoderOptions represent the options for a Decoder.
// The purpose of this type is to unify the input to the different available
// Decoders. The zero value of DecoderOptions means no-prefix/nil-input,
// which should be usable by the Decoders.
type DecoderOptions struct {
	Prefix string
	Input  hcl.Body
}

// EnvDecoder implements Decoder.
type EnvDecoder struct{}

// Decode populates target from the environment.
// The target argument must be a pointer to a struct type value.
func (e *EnvDecoder) Decode(opts *DecoderOptions, target interface{}) error {
	// Decoder Options cannot be missing
	if opts == nil {
		return errors.New("missing DecoderOptions for EnvDecoder")
	}

	// If target is nil then we assume that target is not decodable.
	if target == nil {
		return nil
	}

	envOpts := env.Options{
		Prefix: opts.Prefix, // zero value ok
	}

	return env.Parse(target, envOpts)
}

// HclDecoder implements Decoder.
type HclDecoder struct {
	EvalContext *hcl.EvalContext
}

// Decode populates target given HCL input through DecoderOptions.
// The target argument must be a pointer to an allocated structure.
// If the HCL input is nil, we assume there is nothing to do and the target
// stays unaffected. If the target is nil, we assume is not decodable.
func (h *HclDecoder) Decode(opts *DecoderOptions, target interface{}) error {
	// Decoder Options cannot be missing
	if opts == nil {
		return errors.New("missing DecoderOptions for HclDecoder")
	}

	src := opts.Input
	if src == nil {
		return nil // zero value ok
	}

	// If target is nil then we assume that target is not decodable.
	if target == nil {
		return nil
	}

	// Decode
	diag := gohcl.DecodeBody(src, h.EvalContext, target)
	if len(diag) > 0 {
		return diag
	}

	return nil
}

// CreateHclContext creates an *hcl.EvalContext that is used in decoding HCL.
// Here we can add the evaluation features available for the HCL configuration
// users.
// For now, below is an example of 2 different ways users can reference
// environment variables in their HCL configuration file.
func CreateHclContext() *hcl.EvalContext {
	evalCtx := &hcl.EvalContext{
		Functions: hclCtxFunctions(),
		Variables: hclCtxVariables(),
	}

	return evalCtx
}

// hclCtxFunctions constracts the Functions map of the hcl.EvalContext
// Here, for example, we add the `env` as function.
// Users can reference any env var as `env("MY_ENV_VAR")` e.g.
// ```
// listen_addr = env("LISTEN_ADDR")
// ```
func hclCtxFunctions() map[string]function.Function {
	funcs := map[string]function.Function{
		"env": envFunc(),
	}

	return funcs
}

// hclCtxVariables constracts the Variables map of the hcl.EvalContext
// Here, for example, we add the `env` as variable.
// Users can reference any env var as `env.MY_ENV_VAR` e.g.
// ```
// listen_addr = env.LISTEN_ADDR
// ```
func hclCtxVariables() map[string]cty.Value {
	vars := map[string]cty.Value{
		"env": cty.ObjectVal(envVarsMap(os.Environ())),
	}

	return vars
}

// envFunc constructs a cty.Function that takes a key as string argument and
// returns a string representation of the environment variable behind it.
func envFunc() function.Function {
	return function.New(&function.Spec{
		Params: []function.Parameter{
			{
				Name:         "key",
				Type:         cty.String,
				AllowNull:    false,
				AllowUnknown: false,
			},
		},
		Type: function.StaticReturnType(cty.String),
		Impl: func(args []cty.Value, retType cty.Type) (cty.Value, error) {
			key := args[0].AsString()
			value := os.Getenv(key)
			return cty.StringVal(value), nil
		},
	})
}

// envVarsMap constructs a map of the environment variables to be used in
// hcl.EvalContext
func envVarsMap(environ []string) map[string]cty.Value {
	envMap := make(map[string]cty.Value)
	for _, s := range environ {
		for j := 1; j < len(s); j++ {
			if s[j] == '=' {
				envMap[s[0:j]] = cty.StringVal(s[j+1:])
			}
		}
	}

	return envMap
}
