// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package cloudfunctions

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/twinj/uuid"

	core "github.com/snowplow-devops/stream-replicator/core"
)

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// HandleRequest consumes a Pub/Sub message.
func HandleRequest(ctx context.Context, m PubSubMessage) error {
	cfg, err := core.Init()
	if err != nil {
		return err
	}

	t, err := cfg.GetTarget()
	if err != nil {
		return err
	}
	defer t.Close()

	events := []*core.Event{
		{
			Data:         m.Data,
			PartitionKey: uuid.NewV4().String(),
		},
	}

	err = t.Write(events)
	if err != nil {
		log.Error(err)
	}

	return err
}
