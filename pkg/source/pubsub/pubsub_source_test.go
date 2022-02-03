package pubsubsource

import (
	"os"
	"testing"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/stretchr/testify/assert"
)

func TestGetSource_WithPubsubSource(t *testing.T) {
	assert := assert.New(t)

	supportedSources := []sourceconfig.SourceConfigPair{PubsubSourceConfigPair}

	defer os.Unsetenv("SOURCE")

	os.Setenv("SOURCE", "pubsub")

	pubsubConfig, err := config.NewConfig()
	assert.NotNil(pubsubConfig)
	assert.Nil(err)

	pubsubSource, err := sourceconfig.GetSource(pubsubConfig, supportedSources)

	assert.NotNil(pubsubSource)
	assert.Nil(err)
	assert.Equal("projects//subscriptions/", pubsubSource.GetID())
}
