// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transformconfig

import (
	"fmt"
	"strings"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// GetTransformations builds and returns transformationApplyFunction
// from the transformations configured.
func GetTransformations(c configProvider) (transform.TransformationApplyFunction, error) {
	registry, err := getLayerRegistry()
	if err != nil {
		return nil, err
	}

	transMessage := c.ProvideTransformMessage()
	transUnits, err := parseTransformations(transMessage)
	if err != nil {
		return nil, err
	}

	funcs := make([]transform.TransformationFunction, 0, len(transUnits))
	for _, trans := range transUnits {
		switch trans.name {
		// Builtin transformations
		case "spEnrichedToJson":
			funcs = append(funcs, transform.SpEnrichedToJSON)
		case "spEnrichedSetPk":
			funcs = append(funcs, transform.NewSpEnrichedSetPkFunction(trans.option))
		case "spEnrichedFilter":
			filterFunc, err := transform.NewSpEnrichedFilterFunction(trans.option)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterContext":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionContext(trans.option)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		case "spEnrichedFilterUnstructEvent":
			filterFunc, err := transform.NewSpEnrichedFilterFunctionUnstructEvent(trans.option)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, filterFunc)
		// Custom transformations
		case "lua":
			luaFunc, err := mkEngineFunction(c, trans, registry)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, luaFunc)
		case "js":
			jsFunc, err := mkEngineFunction(c, trans, registry)
			if err != nil {
				return nil, err
			}
			funcs = append(funcs, jsFunc)

			// we don't need `case 'none'` or `default`
			// (see parseTransformations)
		}
	}
	return transform.NewTransformation(funcs...), nil
}

// configProvider is the interface a config must implement to configure the
// stream-replicator transformations
type configProvider interface {
	ProvideTransformMessage() string
	ProvideTransformLayerName() string
	ProvideTransformComponent(p config.Pluggable) (interface{}, error)
}

// transformationUnit is a helper struct type for transformations according to
// the transformation message that is being used to configure the sequence of
// transformations. It denotes the distinction we use when we split by ':',
// e.g. 'spEnrichedSetPk:{option}'
type transformationUnit struct {
	name   string
	option string
}

// layerRegistry is a helper type to map names to the supported Pluggable custom
// transformation layer engines.
type layerRegistry map[string]config.Pluggable

// getLayerRegistry returns the registry of supported Pluggable transform layers.
func getLayerRegistry() (layerRegistry, error) {
	luaLayerPlug, ok := transform.LuaLayer().(config.Pluggable)
	if !ok {
		return nil, fmt.Errorf("non pluggable lua transformation layer")
	}

	jsLayerPlug, ok := transform.JSLayer().(config.Pluggable)
	if !ok {
		return nil, fmt.Errorf("non pluggable js transformation layer")
	}

	return map[string](config.Pluggable){
		"lua": luaLayerPlug,
		"js":  jsLayerPlug,
	}, nil
}

// parseTransformations validates the message_transformation according to rules.
// The reason for this function is to make the validation part explicit and
// separate it from GetTransformations.
func parseTransformations(input string) ([]*transformationUnit, error) {
	if input == "" {
		return nil, fmt.Errorf("invalid message transformation found; empty string")
	}

	transformations := strings.Split(input, ",")
	out := make([]*transformationUnit, 0, len(transformations))
	for _, trans := range transformations {
		splitTrans := strings.Split(trans, ":")
		name := splitTrans[0] // safe

		switch name {
		case "spEnrichedToJson":
			// option rules
			if len(splitTrans) > 1 {
				return nil, fmt.Errorf("invalid message transformation found; unexpected colon after %q", name)
			}

			out = append(out, &transformationUnit{name: name})
		case "spEnrichedSetPk":
			// option rules
			if len(splitTrans) != 2 {
				return nil, fmt.Errorf("invalid message transformation found; expected 'spEnrichedSetPk:{option}' but got %q", trans)
			}

			if splitTrans[1] == "" {
				return nil, fmt.Errorf("invalid message transformation found; empty option for 'spEnrichedSetPk'")
			}

			out = append(out, &transformationUnit{
				name:   name,
				option: splitTrans[1],
			})
		case "spEnrichedFilter":
			// option rules
			if len(splitTrans) != 2 {
				return nil, fmt.Errorf("invalid message transformation found; expected 'spEnrichedFilter:{option}' but got %q", trans)
			}

			if splitTrans[1] == "" {
				return nil, fmt.Errorf("invalid message transformation found; empty option for 'spEnrichedFilter'")
			}

			out = append(out, &transformationUnit{
				name:   name,
				option: splitTrans[1],
			})
		case "spEnrichedFilterContext":
			// option rules
			if len(splitTrans) != 2 {
				return nil, fmt.Errorf("invalid message transformation found; expected 'spEnrichedFilterContext:{option}' but got %q", trans)
			}

			if splitTrans[1] == "" {
				return nil, fmt.Errorf("invalid message transformation found; empty option for 'spEnrichedFilterContext'")
			}

			out = append(out, &transformationUnit{
				name:   name,
				option: splitTrans[1],
			})
		case "spEnrichedFilterUnstructEvent":
			// option rules
			if len(splitTrans) != 2 {
				return nil, fmt.Errorf("invalid message transformation found; expected 'spEnrichedFilterUnstructEvent:{option}' but got %q", trans)
			}

			if splitTrans[1] == "" {
				return nil, fmt.Errorf("invalid message transformation found; empty option for 'spEnrichedFilterUnstructEvent'")
			}

			out = append(out, &transformationUnit{
				name:   name,
				option: splitTrans[1],
			})
		case "lua":
			// option rules
			if len(splitTrans) != 2 {
				return nil, fmt.Errorf("invalid message transformation found; expected 'lua:{option}' but got %q", trans)
			}

			if splitTrans[1] == "" {
				return nil, fmt.Errorf("invalid message transformation found; empty option for 'lua'")
			}

			out = append(out, &transformationUnit{
				name:   name,
				option: splitTrans[1],
			})
		case "js":
			// option rules
			if len(splitTrans) != 2 {
				return nil, fmt.Errorf("invalid message transformation found; expected 'js:{option}' but got %q", trans)
			}

			if splitTrans[1] == "" {
				return nil, fmt.Errorf("invalid message transformation found; empty option for 'js'")
			}

			out = append(out, &transformationUnit{
				name:   name,
				option: splitTrans[1],
			})
		case "none":
			// option rule
			if len(splitTrans) > 1 {
				return nil, fmt.Errorf("invalid message transformation found; unexpected colon after %q", name)
			}
			// none is treated like identity, so ignoring
		case "":
			// this could be caused by some trailing/excessive comma
			// differentiating from default in order to generate a
			// more helpful error message
			return nil, fmt.Errorf("empty transformation found; please check the message transformation syntax")
		default:
			return nil, fmt.Errorf("invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk', 'spEnrichedFilter', 'spEnrichedFilterContext', 'spEnrichedFilterUnstructEvent', 'lua', 'js' or 'none' but got %q", name)
		}
	}

	return out, nil
}

// mkEngineFunction is a helper method used in GetTransformations
// It creates, smoke-tests and returns a custom transformation function.
func mkEngineFunction(c configProvider, trans *transformationUnit, registry layerRegistry) (transform.TransformationFunction, error) {
	useLayerName := c.ProvideTransformLayerName()

	// validate that the expected layer is specified in the configuration
	if useLayerName != trans.name {
		return nil, fmt.Errorf("missing configuration for the custom transformation layer specified: %q", trans.name)
	}

	plug, ok := registry[trans.name]
	if !ok {
		return nil, fmt.Errorf("unknown transformation layer specified")
	}

	component, err := c.ProvideTransformComponent(plug)
	if err != nil {
		return nil, err
	}

	if engine, ok := component.(transform.Engine); ok {
		err := engine.SmokeTest(trans.option)
		if err != nil {
			return nil, err
		}

		return engine.MakeFunction(trans.option), nil
	}

	return nil, fmt.Errorf("could not interpret custom transformation configuration")
}
