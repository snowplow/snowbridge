package cmd

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestServerlessRequestHandler(t *testing.T) {
	os.Clearenv()

	timeNow := time.Now().UTC()
	messages := []*models.Message{
		{
			Data:         []byte("Foo"),
			PartitionKey: "partition1",
			TimeCreated:  timeNow.Add(time.Duration(-50) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-4) * time.Minute),
		},
		{
			Data:         []byte("Bar"),
			PartitionKey: "partition2",
			TimeCreated:  timeNow.Add(time.Duration(-70) * time.Minute),
			TimePulled:   timeNow.Add(time.Duration(-7) * time.Minute),
		},
	}
	err := ServerlessRequestHandler(messages)
	assert.Nil(t, err)
}
