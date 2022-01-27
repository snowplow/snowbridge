// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package awssourceconfig

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/pkg/errors"

	config "github.com/snowplow-devops/stream-replicator/config/common"
	"github.com/snowplow-devops/stream-replicator/pkg/common"
	kinesissource "github.com/snowplow-devops/stream-replicator/pkg/source/kinesis"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// SourceConfigFunctionGeneratorWithInterfaces generates the kinesis Source Config function, allowing you
// to provide a Kinesis + DynamoDB client directly to allow for mocking and localstack usage
func SourceConfigFunctionGeneratorWithInterfaces(kinesisClient kinesisiface.KinesisAPI, dynamodbClient dynamodbiface.DynamoDBAPI, awsAccountID string) func(c *config.Config) (sourceiface.Source, error) {
	// Return a function which returns the source
	return func(c *config.Config) (sourceiface.Source, error) {
		// Handle iteratorTstamp if provided
		var iteratorTstamp time.Time
		var tstampParseErr error
		if c.Sources.Kinesis.StartTimestamp != "" {
			iteratorTstamp, tstampParseErr = time.Parse("2006-01-02 15:04:05.999", c.Sources.Kinesis.StartTimestamp)
			if tstampParseErr != nil {
				return nil, errors.Wrap(tstampParseErr, fmt.Sprintf("Failed to parse provided value for SOURCE_KINESIS_START_TIMESTAMP: %v", iteratorTstamp))
			}
		}

		return kinesissource.NewKinesisSourceWithInterfaces(
			kinesisClient,
			dynamodbClient,
			awsAccountID,
			c.Sources.ConcurrentWrites,
			c.Sources.Kinesis.Region,
			c.Sources.Kinesis.StreamName,
			c.Sources.Kinesis.AppName,
			&iteratorTstamp)
	}
}

// SourceConfigFunction returns a kinesis source config
func SourceConfigFunction(c *config.Config) (sourceiface.Source, error) {
	awsSession, awsConfig, awsAccountID, err := common.GetAWSSession(c.Sources.Kinesis.Region, c.Sources.Kinesis.RoleARN)
	if err != nil {
		return nil, err
	}
	kinesisClient := kinesis.New(awsSession, awsConfig)
	dynamodbClient := dynamodb.New(awsSession, awsConfig)

	sourceConfigFunction := SourceConfigFunctionGeneratorWithInterfaces(
		kinesisClient,
		dynamodbClient,
		*awsAccountID)

	return sourceConfigFunction(c)
}
