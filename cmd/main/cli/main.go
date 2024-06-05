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

package main

import (
	"github.com/snowplow/snowbridge/cmd/cli"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/batchtransform/batchtransformconfig"
	kafkasource "github.com/snowplow/snowbridge/pkg/source/kafka"
	pubsubsource "github.com/snowplow/snowbridge/pkg/source/pubsub"
	sqssource "github.com/snowplow/snowbridge/pkg/source/sqs"
	stdinsource "github.com/snowplow/snowbridge/pkg/source/stdin"
	"github.com/snowplow/snowbridge/pkg/transform/transformconfig"
)

func main() {
	// Make a slice of SourceConfigPairs supported for this build
	sourceConfigPairs := []config.ConfigurationPair{
		stdinsource.ConfigPair, sqssource.ConfigPair,
		kafkasource.ConfigPair, pubsubsource.ConfigPair,
	}

	cli.RunCli(sourceConfigPairs, transformconfig.SupportedTransformations, batchtransformconfig.SupportedTransformations)
}
