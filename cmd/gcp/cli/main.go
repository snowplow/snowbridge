// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"github.com/snowplow-devops/stream-replicator/cmd/cli"
	kafkasource "github.com/snowplow-devops/stream-replicator/pkg/source/kafka"
	pubsubsource "github.com/snowplow-devops/stream-replicator/pkg/source/pubsub"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	sqssource "github.com/snowplow-devops/stream-replicator/pkg/source/sqs"
	stdinsource "github.com/snowplow-devops/stream-replicator/pkg/source/stdin"
)

func main() {
	// Make a slice of SourceConfigPairs supported for this build
	sourceConfigPairs := []sourceconfig.ConfigPair{
		stdinsource.ConfigPair,
		sqssource.ConfigPair,
		pubsubsource.ConfigPair,
		kafkasource.ConfigPair,
	}

	cli.RunCli(sourceConfigPairs)
}
