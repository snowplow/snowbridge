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

package releasetest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/pkg/testutil"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestE2ETargets(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Run("pubsub", testE2EPubsubTarget)
	t.Run("http", testE2EHttpTarget)
	t.Run("http with monitoring alert", testE2EHttpWithMonitoringAlertTarget)
	t.Run("http with monitoring heartbeat", testE2EHttpWithMonitoringHeartbeatTarget)
	t.Run("http with monitoring alert & heartbeat", testE2EHttpWithMonitoringAlertAndHeartbeatTarget)
	t.Run("kinesis", testE2EKinesisTarget)
	t.Run("sqs", testE2ESQSTarget)
	t.Run("kafka", testE2EKafkaTarget)
}

func testE2EPubsubTarget(t *testing.T) {
	assert := assert.New(t)

	topic, subscription := testutil.CreatePubSubTopicAndSubscription(t, "e2e-target-topic", "e2e-target-subscription")
	defer func() {
		if err := topic.Delete(t.Context()); err != nil {
			logrus.Error(err)
		}
	}()
	defer func() {
		if err := subscription.Delete(t.Context()); err != nil {
			logrus.Error(err.Error())
		}
	}()

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "pubsub", "config.hcl"))
	if err != nil {
		panic(err)
	}

	receiverChannel := make(chan string)

	for _, binary := range []string{"-aws-only", ""} {
		// Additional env var options allow us to connect to the pubsub emulator
		_, cmdErr := runDockerCommand(3*time.Second, "pubsubTarget"+binary, configFilePath, binary, "--env PUBSUB_PROJECT_ID=project-test --env PUBSUB_EMULATOR_HOST=integration-pubsub-1:8432")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for PubSub source")
		}

		subReceiver := func(c context.Context, msg *pubsub.Message) {
			receiverChannel <- string(msg.Data) // stringify and pass to channel
		}

		// Receive data in goroutine
		go func() {
			if err := subscription.Receive(t.Context(), subReceiver); err != nil {
				logrus.Error(err.Error())
			}
		}()

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

		evaluateTestCaseString(t, foundData, inputFilePath, "PubSub target "+binary)
	}

}

func testE2EHttpTarget(t *testing.T) {
	assert := assert.New(t)

	// size of 200 to prevent blocking which causes a lot of retrys
	// pattern might be improvable - for now we can adjust to measure if we add more data
	receiverChannel := make(chan string, 200)

	startTestServer := func(wg *sync.WaitGroup) *http.Server {
		srv := &http.Server{Addr: ":8998"}

		http.HandleFunc("/e2e", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			// Extract from array so we don't have to refactor existing JSON evaluate function
			var unmarshalledBody []json.RawMessage

			if err := json.Unmarshal(body, &unmarshalledBody); err != nil {
				panic(err)
			}
			receiverChannel <- string(unmarshalledBody[0])
		})

		go func() {
			defer wg.Done()
			// always returns error. ErrServerClosed on graceful close
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				logrus.Fatalf("ListenAndServe(): %v", err)
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

	for _, binary := range []string{"-aws-only", ""} {

		_, cmdErr := runDockerCommand(10*time.Second, "httpTarget", configFilePath, binary, "")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for HTTP target")
		}

		var foundData []string

	receiveLoop:
		for {
			select {
			case res := <-receiverChannel:
				foundData = append(foundData, res)
			case <-time.After(2 * time.Second):
				break receiveLoop
			}
		}

		expectedFilePath := filepath.Join("cases", "targets", "http", "expected_data.txt")
		evaluateTestCaseJSONString(t, foundData, expectedFilePath, "HTTP target "+binary)
	}

	close(receiverChannel)

	if err := srv.Shutdown(t.Context()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	srvExitWg.Wait()
}

func testE2EKinesisTarget(t *testing.T) {
	assert := assert.New(t)

	appName := "e2eKinesisTarget"

	kinesisClient := testutil.GetAWSLocalstackKinesisClient()

	kinErr := testutil.CreateAWSLocalstackKinesisStream(kinesisClient, appName, 5)
	if kinErr != nil {
		panic(kinErr)
	}
	defer func() {
		if _, err := testutil.DeleteAWSLocalstackKinesisStream(kinesisClient, appName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "kinesis", "config.hcl"))
	if err != nil {
		panic(err)
	}

	streamDescription, err := kinesisClient.DescribeStream(t.Context(), &kinesis.DescribeStreamInput{StreamName: aws.String(appName)})
	if err != nil {
		panic(err)
	}
	shardDescriptions := streamDescription.StreamDescription.Shards
	// Note: if we want to test on streams with more than one shard, this needs to change.

	for _, binary := range []string{"-aws-only", ""} {
		startTstamp := time.Now()
		_, cmdErr := runDockerCommand(3*time.Second, "kinesisTarget", configFilePath, binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for Kinesis target")
		}

		var foundData []string
		// Get data from each shard one by one
		for _, shard := range shardDescriptions {
			iterator, err := kinesisClient.GetShardIterator(t.Context(), &kinesis.GetShardIteratorInput{
				// Shard Id is provided when making put record(s) request.
				ShardId:           shard.ShardId,
				ShardIteratorType: types.ShardIteratorTypeAtTimestamp,
				Timestamp:         &startTstamp,
				StreamName:        aws.String(appName),
			})
			if err != nil {
				panic(err)
			}

			records, err := kinesisClient.GetRecords(t.Context(), &kinesis.GetRecordsInput{
				ShardIterator: iterator.ShardIterator,
			})
			if err != nil {
				panic(err)
			}

			for _, record := range records.Records {
				foundData = append(foundData, string(record.Data))
			}

		}

		// Expected is equal to input.
		evaluateTestCaseString(t, foundData, inputFilePath, "Kinesis target "+binary)

		// Sleep for 1 sec so our timestamp based iterator doesn't overlap tests
		time.Sleep(1 * time.Second)
	}
}

func testE2ESQSTarget(t *testing.T) {
	ctx := t.Context()
	assert := assert.New(t)

	client := testutil.GetAWSLocalstackSQSClient()

	queueName := "sqs-queue-e2e-target"
	out, err := testutil.CreateAWSLocalstackSQSQueue(client, queueName)
	if err != nil {
		panic(err)
	}

	defer func() {
		if _, err := testutil.DeleteAWSLocalstackSQSQueue(client, out.QueueUrl); err != nil {
			logrus.Error(err.Error())
		}
	}()

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "sqs", "config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"-aws-only", ""} {
		_, cmdErr := runDockerCommand(3*time.Second, "sqsTarget", configFilePath, binary, "--env AWS_ACCESS_KEY_ID=foo --env AWS_SECRET_ACCESS_KEY=bar")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error())
		}

		urlResult, err := client.GetQueueUrl(
			ctx,
			&sqs.GetQueueUrlInput{
				QueueName: aws.String(queueName),
			})
		if err != nil {
			panic(err)
		}

		var foundData []string

		// since we can only fetch 10 at a time, loop until we have no data left.
		for {
			msgResult, err := client.ReceiveMessage(
				ctx,
				&sqs.ReceiveMessageInput{
					QueueUrl:            urlResult.QueueUrl,
					MaxNumberOfMessages: 10,
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

func testE2EKafkaTarget(t *testing.T) {
	assert := assert.New(t)

	// We use localhost:9092 here as we're running from host machine.
	// The address in our Snowbridge config is different ("broker:29092"), since they're on the shared docker network.
	adminClient, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	topicName := "e2e-kafka-target"

	err2 := adminClient.CreateTopic(topicName, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false)
	if err2 != nil {
		panic(err2)
	}
	defer func() {
		if err := adminClient.DeleteTopic(topicName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "kafka", "config.hcl"))
	if err != nil {
		panic(err)
	}

	// Set up consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	partitions, err := consumer.Partitions(topicName)
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumer.ConsumePartition(topicName, partitions[0], 0)
	if err != nil {
		panic(err)
	}

	msgChan := partitionConsumer.Messages()

	for _, binary := range []string{"-aws-only", ""} {
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

func testE2EHttpWithMonitoringHeartbeatTarget(t *testing.T) {
	assert := assert.New(t)

	receiverChannel := make(chan string, 2)

	startTestServer := func(wg *sync.WaitGroup) *http.Server {
		srv := &http.Server{Addr: ":6996"}

		http.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()

			time.Sleep(time.Millisecond * 900)
			w.WriteHeader(http.StatusOK)
		})

		http.HandleFunc("/heartbeat-monitoring", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			var unmarshalledBody json.RawMessage
			if err := json.Unmarshal(body, &unmarshalledBody); err != nil {
				panic(err)
			}
			receiverChannel <- string(unmarshalledBody)
		})

		go func() {
			defer wg.Done()
			// always returns error. ErrServerClosed on graceful close
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				logrus.Fatalf("ListenAndServe(): %v", err)
			}
		}()

		// returning reference so caller can call Shutdown()
		return srv
	}

	srvExitWg := &sync.WaitGroup{}

	srvExitWg.Add(1)
	srv := startTestServer(srvExitWg)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "http_with_monitoring", "heartbeat_config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"-aws-only", ""} {

		_, cmdErr := runDockerCommand(3*time.Second, "httpTargetHeartbeat", configFilePath, binary, "")
		if cmdErr == nil {
			assert.Fail("Expected docker run to return an error for HTTP target")
		}

		var foundData []string

	receiveLoop:
		for {
			select {
			case res := <-receiverChannel:
				foundData = append(foundData, res)
				break receiveLoop
			// this is just a precaution to avoid waiting for broken test
			case <-time.After(5020 * time.Millisecond):
				break receiveLoop
			}
		}

		assert.Equal(1, len(foundData))
		assert.Equal(`{"schema":"iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0","data":{"appName":"snowbridge","appVersion":"3.2.3","tags":{"pipeline":"release_tests"}}}`, foundData[0])
	}

	close(receiverChannel)
	if err := srv.Shutdown(t.Context()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	srvExitWg.Wait()
}

func testE2EHttpWithMonitoringAlertTarget(t *testing.T) {
	assert := assert.New(t)

	// we expect exactly 1 alert, because once alert is being sent, nothing else should be coming out of monitoring
	receiverChannel := make(chan string, 1)

	startTestServer := func(wg *sync.WaitGroup) *http.Server {
		srv := &http.Server{Addr: ":7997"}

		http.HandleFunc("/alert", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()
			_, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			http.Error(w, "access to the API is not granted", http.StatusUnauthorized)
		})

		http.HandleFunc("/alert-monitoring", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			var unmarshalledBody json.RawMessage
			if err := json.Unmarshal(body, &unmarshalledBody); err != nil {
				panic(err)
			}
			receiverChannel <- string(unmarshalledBody)
		})

		go func() {
			defer wg.Done()
			// always returns error. ErrServerClosed on graceful close
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				logrus.Fatalf("ListenAndServe(): %v", err)
			}
		}()

		// returning reference so caller can call Shutdown()
		return srv
	}

	srvExitWg := &sync.WaitGroup{}

	srvExitWg.Add(1)
	srv := startTestServer(srvExitWg)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "http_with_monitoring", "alert_config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"-aws-only", ""} {

		_, cmdErr := runDockerCommand(3*time.Second, "httpTargetAlert", configFilePath, binary, "")
		if cmdErr == nil {
			assert.Fail("Expected docker run to return an error for HTTP target")
		}

		var foundData []string

	receiveLoop:
		for {
			select {
			case res := <-receiverChannel:
				foundData = append(foundData, res)
				break receiveLoop
			// this is just a precaution to avoid waiting for broken test
			case <-time.After(1020 * time.Millisecond):
				break receiveLoop
			}
		}

		assert.Equal(1, len(foundData))
		assert.Equal(`{"schema":"iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0","data":{"appName":"snowbridge","appVersion":"3.2.3","tags":{"pipeline":"release_tests"},"message":"1 error occurred:\n\t* got setup error, response status: '401 Unauthorized'\n\n"}}`, foundData[0])
	}

	close(receiverChannel)
	if err := srv.Shutdown(t.Context()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	srvExitWg.Wait()
}

func testE2EHttpWithMonitoringAlertAndHeartbeatTarget(t *testing.T) {
	assert := assert.New(t)
	var counter atomic.Uint64

	// we expect exactly 1 alert and 1 heartbeat
	receiverChannel := make(chan string, 2)
	defer close(receiverChannel)

	startTestServer := func(wg *sync.WaitGroup) *http.Server {
		srv := &http.Server{Addr: ":9999"}

		http.HandleFunc("/alert-heartbeat", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()
			_, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			if counter.Load() < 5 {
				counter.Add(1)
				http.Error(w, "access to the API is not granted", http.StatusUnauthorized)
				return
			}

			time.Sleep(time.Millisecond * 300)
			w.WriteHeader(http.StatusOK)
		})

		http.HandleFunc("/alert-heartbeat-monitoring", func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := r.Body.Close(); err != nil {
					logrus.Error(err.Error())
				}
			}()
			body, err := io.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			var unmarshalledBody json.RawMessage
			if err := json.Unmarshal(body, &unmarshalledBody); err != nil {
				panic(err)
			}
			receiverChannel <- string(unmarshalledBody)
		})

		go func() {
			defer wg.Done()
			// always returns error. ErrServerClosed on graceful close
			if err := srv.ListenAndServe(); err != http.ErrServerClosed {
				logrus.Fatalf("ListenAndServe(): %v", err)
			}
		}()

		// returning reference so caller can call Shutdown()
		return srv
	}

	srvExitWg := &sync.WaitGroup{}

	srvExitWg.Add(1)
	srv := startTestServer(srvExitWg)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "http_with_monitoring", "alert_heartbeat_config.hcl"))
	if err != nil {
		panic(err)
	}

	for _, binary := range []string{"-aws-only", ""} {

		_, cmdErr := runDockerCommand(3*time.Second, "httpTargetAlertHeartbeat", configFilePath, binary, "")
		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for HTTP target")
		}

		var foundData []string

	receiveLoop:
		for {
			select {
			case res := <-receiverChannel:
				foundData = append(foundData, res)
			// this is just a precaution to avoid waiting for broken test
			case <-time.After(1020 * time.Millisecond):
				break receiveLoop
			}
		}

		assert.Equal(2, len(foundData))
		assert.Equal(`{"schema":"iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0","data":{"appName":"snowbridge","appVersion":"3.2.3","tags":{"pipeline":"release_tests"},"message":"1 error occurred:\n\t* got setup error, response status: '401 Unauthorized'\n\n"}}`, foundData[0])
		assert.Equal(`{"schema":"iglu:com.snowplowanalytics.monitoring.loader/heartbeat/jsonschema/1-0-0","data":{"appName":"snowbridge","appVersion":"3.2.3","tags":{"pipeline":"release_tests"}}}`, foundData[1])
	}

	if err := srv.Shutdown(t.Context()); err != nil {
		panic(err) // failure/timeout shutting down the server gracefully
	}

	srvExitWg.Wait()
}
