// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"github.com/aws/aws-lambda-go/events"
)

// Target describes the interface for how to push the data pulled from Kinesis
type Target interface {
	Write(event events.KinesisEvent) error
}
