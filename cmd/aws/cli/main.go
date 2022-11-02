// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"github.com/snowplow/snowbridge/cmd/cli"
	"github.com/snowplow/snowbridge/config"
	kinesissource "github.com/snowplow/snowbridge/pkg/source/kinesis"
	pubsubsource "github.com/snowplow/snowbridge/pkg/source/pubsub"
	sqssource "github.com/snowplow/snowbridge/pkg/source/sqs"
	stdinsource "github.com/snowplow/snowbridge/pkg/source/stdin"
	"github.com/snowplow/snowbridge/pkg/transform/transformconfig"
)

func main() {
	// Make a slice of SourceConfigPairs supported for this build
	sourceConfigPairs := []config.ConfigurationPair{stdinsource.ConfigPair, sqssource.ConfigPair, pubsubsource.ConfigPair, kinesissource.ConfigPair}

	cli.RunCli(sourceConfigPairs, transformconfig.SupportedTransformations)
}
