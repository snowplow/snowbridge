// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package stdinsource

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

func TestStdinSource_ReadSuccess(t *testing.T) {
	assert := assert.New(t)

	// Setup test input
	content := []byte("Hello World!")
	tmpfile, err := ioutil.TempFile("", "example")
	assert.Nil(err)
	defer os.Remove(tmpfile.Name())

	_, err = tmpfile.Write(content)
	assert.Nil(err)
	_, err = tmpfile.Seek(0, 0)
	assert.Nil(err)

	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()
	os.Stdin = tmpfile

	// Read from test input
	source, err := NewStdinSource(1)
	assert.NotNil(source)
	assert.Nil(err)
	assert.Equal("stdin", source.GetID())
	defer source.Stop()

	writeFunc := func(messages []*models.Message) error {
		for _, msg := range messages {
			assert.Equal("Hello World!", string(msg.Data))
		}
		return nil
	}

	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	err1 := source.Read(&sf)
	assert.Nil(err1)
}
