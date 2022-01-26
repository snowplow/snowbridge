// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"github.com/snowplow-devops/stream-replicator/cmd/cli"
	awssourceconfig "github.com/snowplow-devops/stream-replicator/config/aws_source"
)

func main() {
	// We pass awssourceconfig.SourceConfigFunction here to include kinsumer in the build, while other builds don't.
	cli.RunCli(awssourceconfig.SourceConfigFunction)
}
