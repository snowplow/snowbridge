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

package filter

import (
	"errors"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
)

// JQFilterConfig represents the configuration for the JQ filter transformation
type JQFilterConfig struct {
	JQCommand    string `hcl:"jq_command"`
	RunTimeoutMs int    `hcl:"timeout_ms,optional"`
	SpMode       bool   `hcl:"snowplow_mode,optional"`
}

// JQFilterConfigPair is a configuration pair for the jq filter transformation
var JQFilterConfigPair = config.ConfigurationPair{
	Name:   "jqFilter",
	Handle: jqFilterAdapterGenerator(jqFilterConfigFunction),
}

func jqFilterConfigFunction(cfg *JQFilterConfig) (transform.TransformationFunction, error) {
	return transform.GojqTransformationFunction(cfg.JQCommand, cfg.RunTimeoutMs, cfg.SpMode, filterOutput)
}

func jqFilterAdapterGenerator(f func(*JQFilterConfig) (transform.TransformationFunction, error)) jqFilterAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*JQFilterConfig)
		if !ok {
			return nil, errors.New("invalid input, expected JQFilterConfig")
		}

		return f(cfg)
	}
}

// This is where actual filtering is implemented, based on a JQ command output.
func filterOutput(jqOutput transform.JqCommandOutput) transform.TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		shouldKeepMessage, isBoolean := jqOutput.(bool)

		// maybe crash instead?
		if !isBoolean {
			message.SetError(errors.New("jq filter doesn't evaluate to boolean value"))
			return nil, nil, message, nil
		}

		if !shouldKeepMessage {
			return nil, message, nil, nil
		}

		return message, nil, nil, interState
	}
}

type jqFilterAdapter func(i interface{}) (interface{}, error)

func (f jqFilterAdapter) ProvideDefault() (interface{}, error) {
	return &JQFilterConfig{
		RunTimeoutMs: 100,
	}, nil
}

func (f jqFilterAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}
