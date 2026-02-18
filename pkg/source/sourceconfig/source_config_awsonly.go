//go:build awsonly

// This version of source config imports kinsumer, and is only included in aws only builds.

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

package sourceconfig

import (
	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	kinesissource "github.com/snowplow/snowbridge/v3/pkg/source/kinesis"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

// GetSource creates and returns the source that is configured, along with the message channel for the transformer to read from.
func GetSource(
	c *config.Config,
	obs *observer.Observer,
) (sourceiface.Source, chan *models.Message, error) {
	useSource := c.Data.Source.Use

	var source sourceiface.Source
	var err error

	switch useSource.Name {
	case "kinesis":
		decoderOpts := &config.DecoderOptions{
			Input: useSource.Body,
		}
		cfg := kinesissource.DefaultConfiguration()
		if err := c.Decoder.Decode(decoderOpts, &cfg); err != nil {
			return nil, nil, err
		}
		source, err = kinesissource.BuildFromConfig(&cfg, obs)
		if err != nil {
			return nil, nil, err
		}
	default:
		source, err = sourceCommon(c)
		if err != nil {
			return nil, nil, err
		}
	}

	// The source is the sole producer to the output channel, so ownership clearly lies here.
	outputChannel := make(chan *models.Message)

	source.SetChannels(outputChannel)

	return source, outputChannel, nil
}
