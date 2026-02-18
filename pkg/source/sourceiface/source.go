/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package sourceiface

import (
	"context"

	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// Source describes the interface for how to reading the data from a source and writing it to an output channel.
type Source interface {
	// Channel management methods, these are provided by the SourceChannel implementation below, that just needs to be embedded.
	SetChannels(messageChannel chan<- *models.Message)

	// Start is a long-running process to read from the source and write messages to the output channel.
	// It should respect context cancellation for graceful shutdown.
	Start(ctx context.Context)
}

// Struct type to embed into the source driver, exists mostly to have channel management in one place.
type SourceChannels struct {
	MessageChannel chan<- *models.Message
}

func (sc *SourceChannels) SetChannels(messageChannel chan<- *models.Message) {
	sc.MessageChannel = messageChannel
}
