// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"context"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/snowplow-devops/stream-replicator/internal"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

func main() {
	lambda.Start(HandleRequest)
}

// HandleRequest processes the Kinesis event and forwards it onto another stream
func HandleRequest(ctx context.Context, event events.KinesisEvent) error {
	cfg, sentryEnabled, err := internal.Init()
	if err != nil {
		return err
	}
	if sentryEnabled {
		defer sentry.Flush(2 * time.Second)
	}

	t, err := cfg.GetTarget()
	if err != nil {
		return err
	}
	t.Open()

	messages := make([]*models.Message, len(event.Records))
	for i := 0; i < len(messages); i++ {
		record := event.Records[i]
		messages[i] = &models.Message{
			Data:         record.Kinesis.Data,
			PartitionKey: record.Kinesis.PartitionKey,
		}
	}

	_, err = t.Write(messages)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error(err)
	}

	t.Close()
	return err
}
