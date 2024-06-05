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

package batchtransformconfig

import (
	"fmt"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/batchtransform"
)

// SupportedTransformations is a ConfigurationPair slice containing all the officially supported transformations.
var SupportedTransformations = []config.ConfigurationPair{
	// TODO: Add config implementations & put them here
}

// GetBatchTransformations builds and returns transformationApplyFunction
// from the transformations configured.
func GetBatchTransformations(c *config.Config, supportedTransformations []config.ConfigurationPair) (batchtransform.BatchTransformationApplyFunction, error) {
	funcs := make([]batchtransform.BatchTransformationFunction, 0)

	for _, transformation := range c.Data.BatchTransformations {

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

		f, ok := component.(batchtransform.BatchTransformationFunction)
		if !ok {
			return nil, fmt.Errorf("could not interpret transformation configuration for %q", useTransf.Name)
		}
		funcs = append(funcs, f)
	}

	return batchtransform.NewBatchTransformation(funcs...), nil
}
