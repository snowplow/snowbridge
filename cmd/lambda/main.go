// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	log "github.com/sirupsen/logrus"

	core "github.com/snowplow-devops/stream-replicator/core"
)

func main() {
	lambda.Start(HandleRequest)
}

// HandleRequest processes the Kinesis event and forwards it onto another stream
func HandleRequest(ctx context.Context, event events.KinesisEvent) error {
	cfg, err := core.Init()
	if err != nil {
		log.Panicf(err.Error())
	}

	// Build target client
	t, err := cfg.GetTarget()
	if err != nil {
		log.Panicf("FATAL: config.GetTarget: %s", err.Error())
	}

	// Parse events
	events := make([]*core.Event, len(event.Records))
	for i := 0; i < len(events); i++ {
		record := event.Records[i]
		events[i] = &core.Event{
			Data:         record.Kinesis.Data,
			PartitionKey: record.Kinesis.PartitionKey,
		}
	}

	// Write events
	err = t.Write(events)
	if err != nil {
		log.Error(err)
	}

	return err
}
