//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transformconfig

import (
	"fmt"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/snowplow/snowbridge/pkg/transform/engine"
	"github.com/snowplow/snowbridge/pkg/transform/filter"
)

// SupportedTransformations is a ConfigurationPair slice containing all the officially supported transformations.
var SupportedTransformations = []config.ConfigurationPair{
	filter.AtomicFilterConfigPair,
	filter.UnstructFilterConfigPair,
	filter.ContextFilterConfigPair,
	transform.SetPkConfigPair,
	transform.EnrichedToJSONConfigPair,
	transform.Base64DecodeConfigPair,
	transform.Base64EncodeConfigPair,
	engine.LuaConfigPair,
	engine.JSConfigPair,
}

// GetTransformations builds and returns transformationApplyFunction
// from the transformations configured.
func GetTransformations(c *config.Config, supportedTransformations []config.ConfigurationPair) (transform.TransformationApplyFunction, error) {
	funcs := make([]transform.TransformationFunction, 0)

	for _, transformation := range c.Data.Transformations {

		useTransf := transformation.Use
		decoderOpts := &config.DecoderOptions{
			Input: useTransf.Body,
		}

		var component interface{}
		var err error
		for _, pair := range supportedTransformations {
			if pair.Name == useTransf.Name {
				plug := pair.Handle
				component, err = c.CreateComponent(plug, decoderOpts)
				if err != nil {
					return nil, err
				}
			}
		}

		f, ok := component.(transform.TransformationFunction)
		if !ok {
			return nil, fmt.Errorf("could not interpret transformation configuration for %q", useTransf.Name)
		}
		funcs = append(funcs, f)
	}

	return transform.NewTransformation(funcs...), nil
}
