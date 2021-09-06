// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package source

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestKinesisSource_ReadFailure_NoResources(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	source, err := NewKinesisSourceWithInterfaces(kinesisClient, dynamodbClient, "00000000000", 1, testutil.AWSLocalstackRegion, "not-exists", "fake-name", nil)
	assert.Nil(err)
	assert.NotNil(source)
	assert.Equal("arn:aws:kinesis:us-east-1:00000000000:stream/not-exists", source.GetID())

	err = source.Read(nil)
	assert.NotNil(err)
	assert.Equal("Failed to start Kinsumer client: error describing table fake-name_checkpoints: ResourceNotFoundException: Cannot do operations on a non-existent table", err.Error())
}
