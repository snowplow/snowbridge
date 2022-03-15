// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package stdinsource

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
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
	source, err := newStdinSource(1)
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

func TestGetSource_WithStdinSource(t *testing.T) {
	assert := assert.New(t)

	supportedSources := []sourceconfig.ConfigPair{ConfigPair}

	defer os.Unsetenv("SOURCE_NAME")

	os.Setenv("SOURCE_NAME", "stdin")

	c, err := config.NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	stdinSource, err := sourceconfig.GetSource(c, supportedSources)

	assert.NotNil(stdinSource)
	assert.Nil(err)
	assert.Equal("stdin", stdinSource.GetID())
}
