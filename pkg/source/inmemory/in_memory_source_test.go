/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package inmemory

import (
	"sync"
	"testing"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
	"github.com/stretchr/testify/assert"
)

func TestInMemorySource_ReadSuccess(t *testing.T) {
	assert := assert.New(t)

	wg := sync.WaitGroup{}
	inputChannel := make(chan []string)
	source, err := newInMemorySource(inputChannel)
	assert.NotNil(source)
	assert.Nil(err)
	assert.Equal("inMemory", source.GetID())
	defer source.Stop()

	var out []string

	writeFunc := func(messages []*models.Message) error {
		for _, msg := range messages {
			out = append(out, string(msg.Data))
			wg.Done()
		}
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	go func() {
		err1 := source.Read(&sf)
		assert.Nil(err1)
	}()

	wg.Add(6)
	inputChannel <- []string{"m1", "m2"}
	inputChannel <- []string{"m3", "m4", "m5"}
	inputChannel <- []string{"m6"}
	wg.Wait()

	assert.Equal([]string{"m1", "m2", "m3", "m4", "m5", "m6"}, out)
}
