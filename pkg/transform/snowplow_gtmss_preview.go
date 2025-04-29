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

package transform

import (
	"encoding/base64"
	"errors"
	"time"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"

	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// GTMSSPreviewConfig is a configuration object for the spEnrichedToJson transformation
type GTMSSPreviewConfig struct {
	Expiry int `hcl:"expiry_seconds,optional"`
}

// The gtmssPreviewAdapter implements the Pluggable interface
type gtmssPreviewAdapter func(i interface{}) (interface{}, error)

// ProvideDefault implements the ComponentConfigurable interface
func (f gtmssPreviewAdapter) ProvideDefault() (interface{}, error) {
	cfg := &GTMSSPreviewConfig{Expiry: 300} // seconds -> 5 minutes
	return cfg, nil
}

// Create implements the ComponentCreator interface.
func (f gtmssPreviewAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// gtmssPreviewAdapterGenerator returns a gtmssPreviewAdapter
func gtmssPreviewAdapterGenerator(f func(cfg *GTMSSPreviewConfig) (TransformationFunction, error)) gtmssPreviewAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*GTMSSPreviewConfig)
		if !ok {
			return nil, errors.New("unexpected configuration input for gtmssPreview transformation")
		}

		return f(cfg)
	}
}

// gtmssPreviewConfigFunction returns a transformation function
func gtmssPreviewConfigFunction(cfg *GTMSSPreviewConfig) (TransformationFunction, error) {
	ctx := "contexts_com_google_tag-manager_server-side_preview_mode_1"
	property := "x-gtm-server-preview"
	header := "x-gtm-server-preview"
	expiry := time.Duration(cfg.Expiry) * time.Second
	return gtmssPreviewTransformation(ctx, property, header, expiry), nil
}

// GTMSSPreviewConfigPair is the configuration pair for the gtmss preview transformation
var GTMSSPreviewConfigPair = config.ConfigurationPair{
	Name:   "spGtmssPreview",
	Handle: gtmssPreviewAdapterGenerator(gtmssPreviewConfigFunction),
}

// gtmssPreviewTransformation returns a transformation function
func gtmssPreviewTransformation(ctx, property, headerKey string, expiry time.Duration) TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		parsedEvent, err := IntermediateAsSpEnrichedParsed(interState, message)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		headerVal, err := extractHeaderValue(parsedEvent, ctx, property)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		if headerVal != nil {
			tstamp, err := parsedEvent.GetValue("collector_tstamp")
			if err != nil {
				message.SetError(err)
				return nil, nil, message, nil
			}

			if collectorTstamp, ok := tstamp.(time.Time); ok {
				if time.Now().UTC().After(collectorTstamp.Add(expiry)) {
					message.SetError(errors.New("message has expired"))
					return nil, nil, message, nil
				}
			}

			if message.HTTPHeaders == nil {
				message.HTTPHeaders = make(map[string]string)
			}
			message.HTTPHeaders[headerKey] = *headerVal
			return message, nil, nil, parsedEvent
		}

		return message, nil, nil, parsedEvent
	}
}

func extractHeaderValue(parsedEvent analytics.ParsedEvent, ctx, prop string) (*string, error) {
	values, err := parsedEvent.GetContextValue(ctx, prop)
	if err != nil {
		return nil, err
	}

	headerVals, ok := values.([]interface{})
	if !ok {
		// this is generally not expected to happen
		return nil, errors.New("invalid return type encountered")
	}

	if len(headerVals) > 0 {
		// use only first value found
		headerVal, ok := headerVals[0].(string)
		if !ok {
			return nil, errors.New("invalid header value")
		}

		_, err = base64.StdEncoding.DecodeString(headerVal)
		if err != nil {
			return nil, err
		}
		return &headerVal, nil
	}

	// no value found
	return nil, nil
}
