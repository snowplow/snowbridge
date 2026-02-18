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

package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

var (
	// KafkaBrokerEndpoint is the default endpoint for the local Kafka broker
	KafkaBrokerEndpoint = "localhost:9092"
)

// GetKafkaConfig returns a basic Sarama config for testing
func GetKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Consumer.Return.Errors = true
	return config
}

// GetKafkaAdminClient returns a Kafka admin client for managing topics
func GetKafkaAdminClient() (sarama.ClusterAdmin, error) {
	config := GetKafkaConfig()
	return sarama.NewClusterAdmin([]string{KafkaBrokerEndpoint}, config)
}

// GetKafkaSyncProducer returns a Kafka sync producer for publishing test messages
func GetKafkaSyncProducer() (sarama.SyncProducer, error) {
	config := GetKafkaConfig()
	return sarama.NewSyncProducer([]string{KafkaBrokerEndpoint}, config)
}

// CreateKafkaTopic creates a new Kafka topic with the specified number of partitions
func CreateKafkaTopic(admin sarama.ClusterAdmin, topicName string, numPartitions int32, replicationFactor int16) error {
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	err := admin.CreateTopic(topicName, topicDetail, false)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topicName, err)
	}

	// Wait for topic to be ready
	return waitForTopicReady(admin, topicName, 10*time.Second)
}

// DeleteKafkaTopic deletes an existing Kafka topic
func DeleteKafkaTopic(admin sarama.ClusterAdmin, topicName string) error {
	return admin.DeleteTopic(topicName)
}

// PutProvidedDataIntoKafka puts the provided data into a Kafka topic
func PutProvidedDataIntoKafka(producer sarama.SyncProducer, topicName string, data []string) error {
	for i, msgData := range data {
		msg := &sarama.ProducerMessage{
			Topic: topicName,
			Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i)),
			Value: sarama.StringEncoder(msgData),
		}

		_, _, err := producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("failed to send message %d: %w", i, err)
		}
	}
	return nil
}

// waitForTopicReady polls the Kafka cluster until the topic is ready or timeout occurs
func waitForTopicReady(admin sarama.ClusterAdmin, topicName string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for topic %s to be ready", topicName)
		case <-ticker.C:
			metadata, err := admin.DescribeTopics([]string{topicName})
			if err != nil {
				continue
			}

			if len(metadata) > 0 && metadata[0].Err == sarama.ErrNoError {
				return nil
			}
		}
	}
}
