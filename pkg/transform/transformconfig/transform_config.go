// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transformconfig

import (
	"fmt"
	"regexp"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
)

// Transformation represents a transformation's configuration
type Transformation struct {
	// For native filters
	Description               string `hcl:"description,optional"`
	UnstructEventName         string `hcl:"unstruct_event_name,optional"`
	UnstructEventVersionRegex string `hcl:"unstruct_event_version_regex,optional"`
	ContextFullName           string `hcl:"context_full_name,optional"`
	CustomFieldPath           string `hcl:"custom_field_path,optional"`
	AtomicField               string `hcl:"atomic_field,optional"`
	Regex                     string `hcl:"regex,optional"`
	FilterAction              string `hcl:"filter_action,optional"`
	// for JS and Lua transformations
	SourceB64         string `hcl:"source_b64,optional"`
	TimeoutSec        int    `hcl:"timeout_sec,optional"`
	Sandbox           bool   `hcl:"sandbox,optional"`
	SpMode            bool   `hcl:"snowplow_mode,optional"`
	DisableSourceMaps bool   `hcl:"disable_source_maps,optional"`

	Engine engine.Engine
	Name   string
}

// TransformationAdapter is an adapter for transformations to be used
// as pluggable components. It implements the Pluggable interface.
type TransformationAdapter func(i interface{}) (interface{}, error)

// ProvideDefault returns an empty Transformation to be used as default
func (t TransformationAdapter) ProvideDefault() (interface{}, error) {
	return &Transformation{}, nil
}

// Create implements the ComponentCreator interface
func (t TransformationAdapter) Create(i interface{}) (interface{}, error) {
	return t(i)
}

// TransformationConfigFunction creates a Transformation from a TransformationConfig
func TransformationConfigFunction(c *Transformation) (*Transformation, error) {
	return c, nil
}

// AdaptTransformationsFunc returns an TransformationsAdapter.
func AdaptTransformationsFunc(f func(c *Transformation) (*Transformation, error)) TransformationAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*Transformation)
		if !ok {
			return nil, errors.New("invalid input, expected Transformation")
		}

		return f(cfg)
	}
}

// ValidateTransformations validates the transformation according to rules.
// The reason for this function is to make the validation part explicit and
// separate it from GetTransformations.
func ValidateTransformations(transformations []*Transformation) []error {
	var validationErrors []error
	for idx, transformation := range transformations {
		switch transformation.Name {
		case "spEnrichedToJson":
			continue
		case "spEnrichedSetPk":
			if transformation.AtomicField == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedSetPk, empty atomic field`, idx))
				continue
			}
		case "spEnrichedFilter":
			if transformation.AtomicField == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilter, empty atomic field`, idx))
			}
			if transformation.Regex == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilter, empty regex`, idx))
			}
			if transformation.FilterAction != "keep" && transformation.FilterAction != "drop" {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilter, filter_action must be 'keep' or 'drop'`, idx))
			} else {
				_, err := regexp.Compile(transformation.Regex)
				if err != nil {
					validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilter, regex does not compile. error: %v`, idx, err))
				}
			}
			continue
		case "spEnrichedFilterContext":
			if transformation.ContextFullName == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterContext, empty context full name`, idx))
			}
			if transformation.CustomFieldPath == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterContext, empty custom field path`, idx))
			}
			if transformation.Regex == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterContext, empty regex`, idx))
			}
			if transformation.FilterAction != "keep" && transformation.FilterAction != "drop" {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilter, filter_action must be 'keep' or 'drop'`, idx))
			} else {
				_, err := regexp.Compile(transformation.Regex)
				if err != nil {
					validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterContext, regex does not compile. error: %v`, idx, err))
				}
			}
			continue
		case "spEnrichedFilterUnstructEvent":
			if transformation.CustomFieldPath == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterUnstructEvent, empty custom field path`, idx))
			}
			if transformation.UnstructEventName == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterUnstructEvent, empty event name`, idx))
			}
			if transformation.Regex == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterUnstructEvent, empty regex`, idx))
			}
			if transformation.FilterAction != "keep" && transformation.FilterAction != "drop" {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilter, filter_action must be 'keep' or 'drop'`, idx))
			} else {
				_, err := regexp.Compile(transformation.Regex)
				if err != nil {
					validationErrors = append(validationErrors, fmt.Errorf(`validation error #%d spEnrichedFilterUnstructEvent, regex does not compile. error: %v`, idx, err))
				}
			}
			continue
		case "lua":
			if transformation.Engine.SmokeTest(`main`) != nil {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error in lua transformation #%d, main() smoke test failed`, idx))
				continue
			}
		case "js":
			if transformation.Engine.SmokeTest(`main`) != nil {
				validationErrors = append(validationErrors, fmt.Errorf(`validation error in js transformation #%d, main() smoke test failed`, idx))
				continue
			}
		default:
			validationErrors = append(validationErrors, fmt.Errorf(`invalid transformation name: %s`, transformation.Name))
		}
	}
	return validationErrors
}

// MkEngineFunction is a helper method used in GetTransformations
// It creates, smoke-tests and returns a custom transformation function.
func MkEngineFunction(trans *Transformation) (transform.TransformationFunction, error) {
	if trans.Engine != nil {
		return trans.Engine.MakeFunction(`main`), nil
	}

	return nil, errors.New(`could not find engine for transformation`)
}

// GetTransformations builds and returns transformationApplyFunction
// from the transformations configured.
func GetTransformations(c *config.Config) (transform.TransformationApplyFunction, error) {
	transformations := make([]*Transformation, len(c.Data.Transformations))
	for idx, transformation := range c.Data.Transformations {
		var enginePlug config.Pluggable
		var eng engine.Engine
		decoderOpts := &config.DecoderOptions{
			Input: transformation.Use.Body,
		}
		if transformation.Use.Name == `lua` {
			enginePlug = engine.AdaptLuaEngineFunc(engine.LuaEngineConfigFunction)
			component, err := c.CreateComponent(enginePlug, decoderOpts)
			if err != nil {
				return nil, err
			}

			engine, ok := component.(engine.Engine)
			if !ok {
				return nil, errors.New("cannot create lua engine")
			}
			eng = engine
		}
		if transformation.Use.Name == `js` {
			enginePlug = engine.AdaptJSEngineFunc(engine.JSEngineConfigFunction)
			component, err := c.CreateComponent(enginePlug, decoderOpts)
			if err != nil {
				return nil, err
			}

			engine, ok := component.(engine.Engine)
			if !ok {
				return nil, errors.New("cannot create js engine")
			}
			eng = engine
		}

		plug := AdaptTransformationsFunc(TransformationConfigFunction)

		component, err := c.CreateComponent(plug, &config.DecoderOptions{
			Input: transformation.Use.Body,
		})
		if err != nil {
			return nil, err
		}

		trans, ok := component.(*Transformation)
		if !ok {
			return nil, fmt.Errorf(`error parsing transformation: %s`, transformation.Use.Name)
		}
		if eng != nil {
			trans.Engine = eng
		}
		trans.Name = transformation.Use.Name
		transformations[idx] = trans
	}

	validationErrors := ValidateTransformations(transformations)
	if validationErrors != nil {
		for _, err := range validationErrors {
			log.Errorf("validation error: %v", err)
		}
		return nil, errors.New(`transformations validation returned errors`)
	}

	funcs := make([]transform.TransformationFunction, 0, len(transformations))
	for _, transformation := range transformations {
		switch transformation.Name {
		// Builtin transformations
		case "spEnrichedToJson":
			funcs = append(funcs, transform.SpEnrichedToJSON)
		case "spEnrichedSetPk":
			funcs = append(funcs, transform.NewSpEnrichedSetPkFunction(transformation.AtomicField))
		case "spEnrichedFilter":
			filterFunc, err := transform.NewSpEnrichedFilterFunction(transformation.AtomicField, transformation.Regex, transformation.FilterAction)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterContext":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionContext(transformation.ContextFullName, transformation.CustomFieldPath, transformation.Regex, transformation.FilterAction)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterUnstructEvent":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionUnstructEvent(transformation.UnstructEventName, transformation.UnstructEventVersionRegex, transformation.CustomFieldPath, transformation.Regex, transformation.FilterAction)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		// Custom transformations
		case "lua":
			luaFunc, err := MkEngineFunction(transformation)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, luaFunc)
		case "js":
			jsFunc, err := MkEngineFunction(transformation)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, jsFunc)
		}
	}

	return transform.NewTransformation(funcs...), nil
}
