// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package sqssource

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	config "github.com/snowplow-devops/stream-replicator/config/common"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceconfig"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestSQSSource_ReadFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	source, err := NewSQSSourceWithInterfaces(client, "00000000000", 1, testutil.AWSLocalstackRegion, "not-exists")
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:sqs:us-east-1:00000000000:not-exists", source.GetID())

	err = source.Read(nil)
	assert.NotNil(err)
}

func TestSQSSource_ReadSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-source"
	queueURL := testutil.SetupAWSLocalstackSQSQueueWithMessages(client, queueName, 50, "Hello SQS!!")
	defer testutil.DeleteAWSLocalstackSQSQueue(client, queueURL)

	source, err := NewSQSSourceWithInterfaces(client, "00000000000", 10, testutil.AWSLocalstackRegion, queueName)
	assert.Nil(err)
	assert.NotNil(source)

	messageCount := 0
	writeFunc := func(messages []*models.Message) error {
		for _, msg := range messages {
			assert.Equal("Hello SQS!!", string(msg.Data))
			messageCount++

			msg.AckFunc()
		}
		return nil
	}
	sf := sourceiface.SourceFunctions{
		WriteToTarget: writeFunc,
	}

	done := make(chan bool)
	go func() {
		err = source.Read(&sf)
		assert.Nil(err)

		done <- true
	}()

	// Wait for the reader to process a batch
	time.Sleep(1 * time.Second)
	source.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		panic("TestSQSSource_ReadSuccess timed out!")
	}

	assert.Equal(50, messageCount)
}

func TestGetSource_WithSQSSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	sqsClient := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-source-config-integration-1"
	_, createErr := testutil.CreateAWSLocalstackSQSQueue(sqsClient, queueName)
	if createErr != nil {
		panic(createErr)
	}

	defer testutil.DeleteAWSLocalstackSQSQueue(sqsClient, &queueName)

	defer os.Unsetenv("SOURCE")

	os.Setenv("SOURCE", "sqs")
	os.Setenv("SOURCE_SQS_QUEUE_NAME", queueName)

	c, err := config.NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	sqsSourceConfigFunctionWithLocalStack := SQSSourceConfigFunctionGeneratorWithInterfaces(sqsClient, "00000000000")
	sqsSourceConfigPairWithInterfaces := sourceconfig.SourceConfigPair{SourceName: "sqs", SourceConfigFunc: sqsSourceConfigFunctionWithLocalStack}
	supportedSources := []sourceconfig.SourceConfigPair{sqsSourceConfigPairWithInterfaces}

	source, err := sourceconfig.GetSource(c, supportedSources)
	assert.NotNil(source)
	assert.Nil(err)

	assert.IsType(&SQSSource{}, source)
}
