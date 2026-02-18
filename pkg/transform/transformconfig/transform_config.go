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

package transformconfig

import (
	"fmt"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	"github.com/snowplow/snowbridge/v3/pkg/transform"
	"github.com/snowplow/snowbridge/v3/pkg/transform/engine"
	"github.com/snowplow/snowbridge/v3/pkg/transform/filter"
)

// SupportedTransformations is a ConfigurationPair slice containing all the officially supported transformations.
var SupportedTransformations = []config.ConfigurationPair{
	filter.AtomicFilterConfigPair,
	filter.UnstructFilterConfigPair,
	filter.ContextFilterConfigPair,
	filter.JQFilterConfigPair,
	transform.SetPkConfigPair,
	transform.EnrichedToJSONConfigPair,
	transform.Base64DecodeConfigPair,
	transform.Base64EncodeConfigPair,
	transform.GTMSSPreviewConfigPair,
	transform.JQMapperConfigPair,
	engine.JSConfigPair,
}

// GetTransformations builds and returns transformationApplyFunction
// from the transformations configured.
func GetTransformations(c *config.Config, supportedTransformations []config.ConfigurationPair) (transform.TransformationApplyFunction, error) {
	funcs := make([]transform.TransformationFunction, 0)

	if c.Data.Metrics.E2ELatencyEnabled {
		funcs = append(funcs, transform.CollectorTstampTransformation())
	}

	if c.Data.Transform != nil {
		for _, transformation := range c.Data.Transform.Transformations {

			decoderOpts := &config.DecoderOptions{
				Input: transformation.Body,
			}

			var component any
			var err error
			for _, pair := range supportedTransformations {
				if pair.Name == transformation.Name {
					plug := pair.Handle
					component, err = c.CreateComponent(plug, decoderOpts)
					if err != nil {
						return nil, err
					}
				}
			}

			f, ok := component.(transform.TransformationFunction)
			if !ok {
				return nil, fmt.Errorf("could not interpret transformation configuration for %q", transformation.Name)
			}
			funcs = append(funcs, f)
		}
	}

	return transform.NewTransformation(funcs...), nil
}

// GetTransformer builds and returns a complete Transformer with all channels configured, along with the output channel for the router to read from.
func GetTransformer(
	c *config.Config,
	supportedTransformations []config.ConfigurationPair,
	input <-chan *models.Message,
	obs *observer.Observer,
) (*transform.Transformer, chan *models.TransformationResult, error) {
	// Get the transformation function
	transformFunc, err := GetTransformations(c, supportedTransformations)
	if err != nil {
		return nil, nil, err
	}

	// Get worker pool config, defaulting to 0 if not set
	workerPool := 0
	if c.Data.Transform != nil {
		workerPool = c.Data.Transform.WorkerPool
	}

	// The transformer is the sole producer to the output channel, so ownership clearly lies here.
	output := make(chan *models.TransformationResult)

	// Create and return the transformer and its output channel
	return transform.NewTransformer(transformFunc, input, output, obs, workerPool), output, nil
}
