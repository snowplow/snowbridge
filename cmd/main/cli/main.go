//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package main

import (
	"github.com/snowplow/snowbridge/cmd/cli"
	"github.com/snowplow/snowbridge/config"
	pubsubsource "github.com/snowplow/snowbridge/pkg/source/pubsub"
	sqssource "github.com/snowplow/snowbridge/pkg/source/sqs"
	stdinsource "github.com/snowplow/snowbridge/pkg/source/stdin"
	"github.com/snowplow/snowbridge/pkg/transform/transformconfig"
)

func main() {
	// Make a slice of SourceConfigPairs supported for this build
	sourceConfigPairs := []config.ConfigurationPair{stdinsource.ConfigPair, sqssource.ConfigPair, pubsubsource.ConfigPair}

	cli.RunCli(sourceConfigPairs, transformconfig.SupportedTransformations)
}
