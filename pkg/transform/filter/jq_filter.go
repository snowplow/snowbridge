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

package filter

import (
	"errors"
	"fmt"

	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/transform"
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
	return func(i any) (any, error) {
		cfg, ok := i.(*JQFilterConfig)
		if !ok {
			return nil, errors.New("invalid input, expected JQFilterConfig")
		}

		return f(cfg)
	}
}

// This is where actual filtering is implemented, based on a JQ command output.
func filterOutput(jqOutput transform.JqCommandOutput) transform.TransformationFunction {
	return func(message *models.Message, interState any) (*models.Message, *models.Message, *models.Message, any) {
		shouldKeepMessage, isBoolean := jqOutput.(bool)

		if !isBoolean {
			message.SetError(&models.TransformationError{
				SafeMessage: "jq filter didn't return expected [boolean] value",
				Err:         fmt.Errorf("%v", jqOutput),
			})
			return nil, nil, message, nil
		}

		if !shouldKeepMessage {
			return nil, message, nil, nil
		}

		return message, nil, nil, interState
	}
}

type jqFilterAdapter func(i any) (any, error)

func (f jqFilterAdapter) ProvideDefault() (any, error) {
	return &JQFilterConfig{
		RunTimeoutMs: 100,
	}, nil
}

func (f jqFilterAdapter) Create(i any) (any, error) {
	return f(i)
}
