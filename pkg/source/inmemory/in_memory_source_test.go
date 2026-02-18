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

package inmemory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/v3/pkg/common"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestInMemorySource(t *testing.T) {
	assert := assert.New(t)

	inputChannel := make(chan []string)

	source, err := Build(inputChannel)
	assert.NotNil(source)
	assert.Nil(err)

	outputChannel := make(chan *models.Message, 10)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	inputChannel <- []string{"m1", "m2"}
	inputChannel <- []string{"m3", "m4", "m5"}
	inputChannel <- []string{"m6"}

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	assert.Equal(6, len(successfulReads))

	// Extract message data into a slice for easier comparison
	receivedMessages := make([]string, 0)
	for _, msg := range successfulReads {
		receivedMessages = append(receivedMessages, string(msg.Data))
	}

	assert.Contains(receivedMessages, "m1")
	assert.Contains(receivedMessages, "m2")
	assert.Contains(receivedMessages, "m3")
	assert.Contains(receivedMessages, "m4")
	assert.Contains(receivedMessages, "m5")
	assert.Contains(receivedMessages, "m6")

	cancel()
	assert.True(common.WaitWithTimeout(&wg, 10*time.Second))
}
