package kinesissourceconfig

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	config "github.com/snowplow-devops/stream-replicator/config/common"
	kinesisSource "github.com/snowplow-devops/stream-replicator/pkg/source/kinesis"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
)

func TestNewConfig_WithKinesisSource(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)

	// Set up localstack resources
	kinesisClient := testutil.GetAWSLocalstackKinesisClient()
	dynamodbClient := testutil.GetAWSLocalstackDynamoDBClient()

	streamName := "kinesis-source-confid-integration-1"
	createErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, streamName)
	if createErr != nil {
		panic(createErr)
	}
	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, streamName)

	appName := "kinesisSourceIntegration"
	testutil.CreateAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	defer testutil.DeleteAWSLocalstackDynamoDBTables(dynamodbClient, appName)

	defer os.Unsetenv("SOURCE")

	os.Setenv("SOURCE", "kinesis")

	os.Setenv("SOURCE_KINESIS_STREAM_NAME", streamName)
	os.Setenv("SOURCE_KINESIS_REGION", testutil.AWSLocalstackRegion)
	os.Setenv("SOURCE_KINESIS_APP_NAME", appName)

	c, err := config.NewConfig()
	assert.NotNil(c)
	assert.Nil(err)

	// Use our function generator to interact with localstack
	kinesisSourceConfigFunctionWithInterfaces := SourceConfigFunctionGeneratorWithInterfaces(kinesisClient, dynamodbClient, "00000000000")

	source, err := c.GetSource(kinesisSourceConfigFunctionWithInterfaces)
	assert.NotNil(source)
	assert.Nil(err)

	assert.IsType(&kinesisSource.KinesisSource{}, source)
}
