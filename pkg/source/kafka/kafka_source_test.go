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

package kafkasource

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/assets"
	"github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

func TestKafkaSource_ReadAndReturnSuccessIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	assert := assert.New(t)

	topicName := "kafka_source_integration"

	// Create kafka topic
	adminClient, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := adminClient.Close(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	err2 := adminClient.CreateTopic(topicName,
		&sarama.TopicDetail{NumPartitions: 1,
			ReplicationFactor: 1}, false)
	if err2 != nil {
		panic(err2)
	}
	defer func() {
		if err := adminClient.DeleteTopic(topicName); err != nil {
			logrus.Error(err.Error())
		}
	}()

	// Create a producer
	saramaConfig := sarama.NewConfig()
	// Must be enabled for the SyncProducer
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	producer, producerError := sarama.NewSyncProducer(strings.Split("localhost:9092", ","), saramaConfig)
	if producerError != nil {
		panic(producerError)
	}

	// use it sto send 100 messages in
	for i := 0; i < 100; i++ {
		_, _, sendMessageErr := producer.SendMessage(&sarama.ProducerMessage{
			Topic: topicName,
			Value: sarama.StringEncoder(fmt.Sprint(i)),
		})
		if sendMessageErr != nil {
			panic(sendMessageErr)
		}
	}

	// Configure the kafka source
	filename := filepath.Join(assets.AssetsRootDir, "test", "config", "configs", "empty.hcl")
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

	adaptedHandle := adapterGenerator(configFunction)

	kafkaSourceConfigPair := config.ConfigurationPair{Name: "kafka", Handle: adaptedHandle}
	supportedSources := []config.ConfigurationPair{kafkaSourceConfigPair}

	// Construct the config
	kafkaSourceConfig, err := config.NewConfig()
	assert.NotNil(kafkaSourceConfig)

	if err != nil {
		t.Fatalf("unexpected error: %q", err.Error())
	}

	configBytesToMerge := []byte(fmt.Sprintf(`
    brokers         = "localhost:9092"
    topic_name      = "%s"
    consumer_name   = "integration"
    offsets_initial = "-2"
`, topicName))

	parser := hclparse.NewParser()
	fileHCL, diags := parser.ParseHCL(configBytesToMerge, "placeholder")
	if diags.HasErrors() {
		t.Fatalf("failed to parse config bytes")
	}

	kafkaSourceConfig.Data.Source.Use.Name = "kafka"
	kafkaSourceConfig.Data.Source.Use.Body = fileHCL.Body

	kafkaSource, err := sourceconfig.GetSource(kafkaSourceConfig, supportedSources)

	assert.NotNil(kafkaSource)
	assert.Nil(err)

	// The kafka broker can be slow to get set up for the consumer, making this test flaky.
	// Setting the shut down timer to 10s seems to mitigate this.
	output := testutil.ReadAndReturnMessages(kafkaSource, 10*time.Second, testutil.DefaultTestWriteBuilder, nil)
	assert.Equal(100, len(output))

	// Check that there are no errors in the results and make a slice of the data converted to int
	var found []int
	for _, message := range output {
		assert.Nil(message.GetError())
		intVal, err := strconv.Atoi(string(message.Data))
		if err != nil {
			panic(err)
		}
		found = append(found, intVal)
	}

	// Check for uniqueness
	sort.Ints(found)
	for i, valFound := range found {
		assert.Equal(i, valFound)
	}
}
