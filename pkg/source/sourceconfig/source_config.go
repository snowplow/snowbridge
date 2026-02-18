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
	httpsource "github.com/snowplow/snowbridge/v3/pkg/source/http"

	config "github.com/snowplow/snowbridge/v3/config"
	kafkasource "github.com/snowplow/snowbridge/v3/pkg/source/kafka"
	pubsubsource "github.com/snowplow/snowbridge/v3/pkg/source/pubsub"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceiface"
	sqssource "github.com/snowplow/snowbridge/v3/pkg/source/sqs"
	stdinsource "github.com/snowplow/snowbridge/v3/pkg/source/stdin"
)

func sourceCommon(c *config.Config) (sourceiface.Source, error) {
	useSource := c.Data.Source.Use
	decoderOpts := &config.DecoderOptions{
		Input: useSource.Body,
	}

	switch useSource.Name {
	case stdinsource.SupportedSourceStdin:
		cfg := stdinsource.DefaultConfiguration()
		if err := c.Decoder.Decode(decoderOpts, &cfg); err != nil {
			return nil, err
		}
		return stdinsource.BuildFromConfig(&cfg)
	case kafkasource.SupportedSourceKafka:
		cfg := kafkasource.DefaultConfiguration()
		if err := c.Decoder.Decode(decoderOpts, &cfg); err != nil {
			return nil, err
		}
		return kafkasource.BuildFromConfig(&cfg)
	case pubsubsource.SupportedSourcePubsub:
		cfg := pubsubsource.DefaultConfiguration()
		if err := c.Decoder.Decode(decoderOpts, &cfg); err != nil {
			return nil, err
		}
		return pubsubsource.BuildFromConfig(&cfg)
	case sqssource.SupportedSourceSQS:
		cfg := sqssource.DefaultConfiguration()
		if err := c.Decoder.Decode(decoderOpts, &cfg); err != nil {
			return nil, err
		}
		return sqssource.BuildFromConfig(&cfg)
	case httpsource.SupportedSourceHTTP:
		cfg := httpsource.DefaultConfiguration()
		if err := c.Decoder.Decode(decoderOpts, &cfg); err != nil {
			return nil, err
		}
		return httpsource.BuildFromConfig(&cfg)
	default:
		return nil, fmt.Errorf("unknown source: %s", useSource.Name)
	}
}
