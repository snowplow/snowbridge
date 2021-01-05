// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package cloudfunctions

import (
	"context"
	"github.com/twinj/uuid"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/snowplow-devops/stream-replicator/internal/models"
)

// PubSubMessage is the payload of a Pub/Sub message
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// HandleRequest consumes a Pub/Sub message
func HandleRequest(ctx context.Context, m PubSubMessage) error {
	messages := []*models.Message{
		{
			Data:         m.Data,
			PartitionKey: uuid.NewV4().String(),
		},
	}

	return cmd.ServerlessRequestHandler(messages)
}
