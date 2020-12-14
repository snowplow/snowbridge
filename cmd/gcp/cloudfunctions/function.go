// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package cloudfunctions

import (
	"context"
	"github.com/getsentry/sentry-go"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"
	"time"

	"github.com/snowplow-devops/stream-replicator/internal"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

// PubSubMessage is the payload of a Pub/Sub message
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// HandleRequest consumes a Pub/Sub message
func HandleRequest(ctx context.Context, m PubSubMessage) error {
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

	messages := []*models.Message{
		{
			Data:         m.Data,
			PartitionKey: uuid.NewV4().String(),
		},
	}

	_, err = t.Write(messages)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Error(err)
	}

	t.Close()
	return err
}
