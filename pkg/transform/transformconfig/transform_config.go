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

	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
)

// Transformation represents a transformation's configuration
type Transformation struct {
	Description       string `hcl:"description,optional"`
	Option            string `hcl:"option,optional"`
	Field             string `hcl:"field,optional"`
	Regex             string `hcl:"regex,optional"`
	TimeoutSec        int    `hcl:"timeout_sec,optional"`
	Sandbox           bool   `hcl:"sandbox,optional"`
	SpMode            bool   `hcl:"snowplow_mode,optional"`
	DisableSourceMaps bool   `hcl:"disable_source_maps,optional"`
	EngineName        string `hcl:"engine_name,optional"`

	Name string
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
			if transformation.Option == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedSetPk, empty option`, idx))
				continue
			}
		case "spEnrichedFilter":
			if transformation.Field != `` && transformation.Regex != `` {
				_, err := regexp.Compile(transformation.Regex)
				if err != nil {
					validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilter, regex does not compile. error: %v`, idx, err))
					continue
				}
				continue
			}
			if transformation.Field == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilter, empty field`, idx))
			}
			if transformation.Regex == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilter, empty regex`, idx))
			}
		case "spEnrichedFilterContext":
			if transformation.Field != `` && transformation.Regex != `` {
				_, err := regexp.Compile(transformation.Regex)
				if err != nil {
					validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilterContext, regex does not compile. error: %v`, idx, err))
					continue
				}
				continue
			}
			if transformation.Field == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilterContext, empty field`, idx))
			}
			if transformation.Regex == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilterContext, empty regex`, idx))
			}
		case "spEnrichedFilterUnstructEvent":
			if transformation.Field != `` && transformation.Regex != `` {
				_, err := regexp.Compile(transformation.Regex)
				if err != nil {
					validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilterUnstructEvent, regex does not compile. error: %v`, idx, err))
					continue
				}
				continue
			}
			if transformation.Field == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilterUnstructEvent, empty field`, idx))
			}
			if transformation.Regex == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating transformation #%d spEnrichedFilterUnstructEvent, empty regex`, idx))
			}
		case "lua":
			if transformation.EngineName == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating lua transformation #%d, empty engine name`, idx))
				continue
			}
		case "js":
			if transformation.EngineName == `` {
				validationErrors = append(validationErrors, fmt.Errorf(`error validating js transformation #%d, empty engine name`, idx))
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
func MkEngineFunction(layerRegistry []engine.Engine, trans *Transformation) (transform.TransformationFunction, error) {
	for _, layer := range layerRegistry {
		if layer.GetName() == trans.EngineName {
			return layer.MakeFunction(`main`), nil
		}
	}
	return nil, fmt.Errorf(`could not find engine named %s`, trans.EngineName)
}
