package cmd

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func TestServerlessRequestHandler(t *testing.T) {
	os.Clearenv()
	// simple test writing to stdout, default configuration
	messages := []*models.Message{
		{
			Data:         []byte("Foo"),
			PartitionKey: "test-partition",
			TimeCreated:  time.Now().Add(time.Duration(-30) * time.Minute),
			TimePulled:   time.Now().Add(time.Duration(-10) * time.Minute),
		},
	}
	err := ServerlessRequestHandler(messages)
	assert.Nil(t, err)
}
