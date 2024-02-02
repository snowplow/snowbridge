//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package kafkasource

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/source/sourceconfig"
	"github.com/snowplow/snowbridge/pkg/testutil"

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
	defer adminClient.Close()

	err2 := adminClient.CreateTopic(topicName,
		&sarama.TopicDetail{NumPartitions: 1,
			ReplicationFactor: 1}, false)
	if err2 != nil {
		panic(err2)
	}
	defer adminClient.DeleteTopic(topicName)

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
	t.Setenv("SOURCE_NAME", "kafka")
	t.Setenv("SOURCE_KAFKA_BROKERS", "localhost:9092")
	t.Setenv("SOURCE_KAFKA_TOPIC_NAME", topicName)
	t.Setenv("SOURCE_KAFKA_CONSUMER_NAME", "integration")
	t.Setenv("SOURCE_KAFKA_OFFSETS_INITIAL", "-2")

	adaptedHandle := adapterGenerator(configFunction)

	kafkaSourceConfigPair := config.ConfigurationPair{Name: "kafka", Handle: adaptedHandle}
	supportedSources := []config.ConfigurationPair{kafkaSourceConfigPair}

	kafkaSourceConfig, err := config.NewConfig()
	assert.NotNil(kafkaSourceConfig)
	assert.Nil(err)

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
