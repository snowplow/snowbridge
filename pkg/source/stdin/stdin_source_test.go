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

package stdinsource

import (
	"context"
	"github.com/snowplow/snowbridge/v3/pkg/common"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

func TestStdinSource(t *testing.T) {
	assert := assert.New(t)

	// Setup test input
	content := []byte("Hello World!")
	tmpfile, err := os.CreateTemp("", "example")
	assert.Nil(err)
	defer func() {
		if err := os.Remove(tmpfile.Name()); err != nil {
			logrus.Error(err.Error())
		}
	}()

	_, err = tmpfile.Write(content)
	assert.Nil(err)
	_, err = tmpfile.Seek(0, 0)
	assert.Nil(err)

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()
	os.Stdin = tmpfile

	outputChannel := make(chan *models.Message, 1)

	// Read from test input
	source, err := NewStdinSourceDriver()
	assert.NotNil(source)
	assert.Nil(err)

	source.SetChannels(outputChannel)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		source.Start(ctx)
	})

	successfulReads := testutil.ReadSourceOutput(outputChannel)

	// Check that we got one message and the correct data in the message
	assert.Equal(1, len(successfulReads))
	assert.Equal("Hello World!", string(successfulReads[0].Data))

	// Like in this case, stdin source can quit naturally, without explicit cancel
	assert.True(common.WaitWithTimeout(&wg, 1*time.Second))

	_, ok := <-outputChannel
	assert.False(ok, "Output channel should be closed")

}
