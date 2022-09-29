// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestE2EPubsubTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "e2e-target-topic", "e2e-target-subscription")
	defer topic.Delete(context.Background())
	defer subscription.Delete(context.Background())

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	fmt.Println("Running docker command")

	receiverChannel := make(chan string)

	for _, binary := range []string{"aws", "gcp"} {
		// Additional env var options allow us to connect to the pubsub emulator
		_, cmdErr := runDockerCommand(3*time.Second, "pubsubTarget"+binary, configFilePath, binary, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for PubSub source")
		}

		subReceiver := func(c context.Context, msg *pubsub.Message) {
			receiverChannel <- string(msg.Data) // stringify and pass to channel
		}

		// Receive data in goroutine
		go subscription.Receive(context.TODO(), subReceiver)

		var foundData []string

	receiveLoop:
		for {
			select {
			case res := <-receiverChannel:
				foundData = append(foundData, res)
			case <-time.After(1 * time.Second):
				break receiveLoop // after 1s with no data, break the loop
			}
		}

		expectedFilePath := filepath.Join("cases", "targets", "pubsub", "expected_data.txt")

		evaluateTestCaseString(t, foundData, expectedFilePath, "PubSub target "+binary)
	}

}

func TestE2EHttpTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	var foundData []string

	startTestServer := func(wg *sync.WaitGroup) *http.Server {
		srv := &http.Server{Addr: ":8998"}

		http.HandleFunc("/e2e", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			foundData = append(foundData, string(body))

		})

		go func() {
			defer wg.Done()

			// always returns error. ErrServerClosed on graceful close
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				log.Fatalf("ListenAndServe(): %v", err)
			}
		}()

		// returning reference so caller can call Shutdown()
		return srv
	}

	srvExitWg := &sync.WaitGroup{}

	srvExitWg.Add(1)
	srv := startTestServer(srvExitWg)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "http", "config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"aws", "gcp"} {
		// Additional env var options allow us to connect to the pubsub emulator
		_, cmdErr := runDockerCommand(3*time.Second, "httpTarget", configFilePath, binary, "")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for HTTP target")
		}

		// Expected is equal to input.
		evaluateTestCaseString(t, foundData, inputFilePath, "HTTP target "+binary)

		// Remove data when done, so next iteration starts afresh
		foundData = []string{}
	}

	if err := srv.Shutdown(context.TODO()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	srvExitWg.Wait()
}

func TestE2EKinesisTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	appName := "e2eKinesisTarget"

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()

	kinErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, appName)
	if kinErr != nil {
		panic(kinErr)
	}

	defer testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, appName)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "kinesis", "config.hcl"))
	if err != nil {
		panic(err)
	}

	streamDescription, err := kinesisClient.DescribeStream(&kinesis.DescribeStreamInput{StreamName: aws.String(appName)})
	if err != nil {
		panic(err)
	}
	shardID := streamDescription.StreamDescription.Shards[0].ShardId
	// Note: if we want to test on streams with more than one shard, this needs to change.

	// iterator to start at the beginning.
	iterator, err := kinesisClient.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard Id is provided when making put record(s) request.
		ShardId:           shardID,
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamName:        aws.String(appName),
	})
	if err != nil {
		panic(err)
	}

	seqNum := ""
	for _, binary := range []string{"aws", "gcp"} {
		// Additional env var options allow us to connect to the pubsub emulator
		_, cmdErr := runDockerCommand(3*time.Second, "kinesisTarget", configFilePath, binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for Kinesis target")
		}

		// On the second iteration, start after the last sequence number
		if seqNum != "" {
			iterator, err = kinesisClient.GetShardIterator(&kinesis.GetShardIteratorInput{
				// Shard Id is provided when making put record(s) request.
				ShardId:                shardID,
				ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
				StartingSequenceNumber: aws.String(seqNum),
				StreamName:             aws.String(appName),
			})
			if err != nil {
				panic(err)
			}
		}

		records, err := kinesisClient.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: iterator.ShardIterator,
		})
		if err != nil {
			panic(err)
		}

		var foundData []string
		for _, record := range records.Records {
			if *record.SequenceNumber > seqNum {
				seqNum = *record.SequenceNumber
			}
			foundData = append(foundData, string(record.Data))
		}

		// Expected is equal to input.
		evaluateTestCaseString(t, foundData, inputFilePath, "Kinesis target "+binary)

		// Remove data when done, so next iteration starts afresh
		foundData = []string{}
	}
}

func TestE2ESQSTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-e2e-target"
	out, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}

	defer testutil.DeleteAWSLocalstackSQSQueue(client, out.QueueUrl)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "sqs", "config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"aws", "gcp"} {
		_, cmdErr := runDockerCommand(3*time.Second, "sqsTarget", configFilePath, binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error())
		}

		urlResult, err := client.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(queueName),
		})
		if err != nil {
			panic(err)
		}

		var foundData []string

		// since we can only fetch 10 at a time, loop until we have no data left.
		for {
			msgResult, err := client.ReceiveMessage(&sqs.ReceiveMessageInput{
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl:            urlResult.QueueUrl,
				MaxNumberOfMessages: aws.Int64(10),
			})
			if err != nil {
				panic(err)
			}

			for _, msg := range msgResult.Messages {
				foundData = append(foundData, *msg.Body)
			}

			if len(msgResult.Messages) == 0 {
				break
			}
		}

		// Expected is equal to input.
		evaluateTestCaseString(t, foundData, inputFilePath, "SQS target "+binary)
	}
}

func TestE2EKafkaTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	// Looks like it's better to use cluster admin for the create/delete: https://github.com/Shopify/sarama/blob/v1.36.0/admin.go#L16

	// We use localhost:9092 here as we're running from host machine.
	// The address in our stream replicator config is different ("broker:29092"), since they're on the shared docker network.
	adminClient, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	topicName := "e2e-kafka-target"

	err2 := adminClient.CreateTopic(topicName, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	if err2 != nil {
		panic(err2)
	}
	defer adminClient.DeleteTopic(topicName)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "kafka", "config.hcl"))
	if err != nil {
		panic(err)
	}

	// Set up consumer

	// We use localhost:9092 here as we're running from host machine.
	// The address in our stream replicator config is different ("broker:29092"), since they're on the shared docker network.
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topicName, partitions[0], 0)
	if err != nil {
		panic(err)
	}

	msgChan := partitionConsumer.Messages()

	for _, binary := range []string{"aws", "gcp"} {
		_, cmdErr := runDockerCommand(3*time.Second, "kafkaTarget", configFilePath, binary, "")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error())
		}

		var foundData []string
	receiveLoop:
		for {
			select {
			case res := <-msgChan:
				foundData = append(foundData, string(res.Value))
			case <-time.After(1 * time.Second):
				break receiveLoop // after 1s with no data, break the loop
			}
		}

		// Expected is equal to input.
		evaluateTestCaseString(t, foundData, inputFilePath, "Kafka target "+binary)
	}
	partitionConsumer.AsyncClose()
}
