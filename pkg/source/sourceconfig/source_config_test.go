// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package sourceconfig

import (
	"os"
	"testing"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Clearenv()
	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestNewConfig_InvalidSource(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("SOURCE_NAME", "fake")

	c, err := config.NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	supportedSources := []ConfigPair{}

	source, err := GetSource(c, supportedSources)
	assert.Nil(source)
	assert.NotNil(err)
	assert.Equal("Invalid source found: fake. Supported sources in this build: ", err.Error())
}
