// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"github.com/snowplow-devops/stream-replicator/cmd/cli"
	kinesisSourceConfig "github.com/snowplow-devops/stream-replicator/config/kinesis_source"
)

func main() {
	cli.RunCli(kinesisSourceConfig.SourceConfigFunction)
}
