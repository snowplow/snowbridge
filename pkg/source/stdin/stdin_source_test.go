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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
	config "github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestStdinSource_ReadSuccess(t *testing.T) {
	assert := assert.New(t)

	// Setup test input
	content := []byte("Hello World!")
	tmpfile, err := os.CreateTemp("", "example")
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
	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	assert := assert.New(t)

	supportedSources := []config.ConfigurationPair{ConfigPair}

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	stdinSource, err := sourceconfig.GetSource(c, supportedSources)

	assert.NotNil(stdinSource)
	assert.Nil(err)
	assert.Equal("stdin", stdinSource.GetID())
}
