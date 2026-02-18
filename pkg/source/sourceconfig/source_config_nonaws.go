//go:build !awsonly

// This version of source config does not import kinsumer

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
	"fmt"

	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
)

// GetSource takes a config and some shared resources, and creates a new source, along with the message channel for the transformer to read from.
func GetSource(
	c *config.Config,
	obs *observer.Observer,
) (sourceiface.Source, chan *models.Message, error) {
	useSource := c.Data.Source.Use

	var source sourceiface.Source
	var err error

	switch useSource.Name {

	case "kinesis":
		return nil, nil, fmt.Errorf("kinesis source is not supported in this build, use the aws-only build instead")

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
