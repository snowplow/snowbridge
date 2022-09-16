// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"github.com/pkg/errors"
	"github.com/snowplow-devops/stream-replicator/pkg/target"
)

// Mocks in this file are used in both config tests and docs tests, so we define them here and export them, rather than defining in the tests.

// MockSQSTargetAdapter mocks an SQS Target Adapter to return an SQS config.
func MockSQSTargetAdapter() target.SQSTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.SQSTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected SQSTargetConfig")
		}

		return cfg, nil
	}

}

// MockEventHubTargetAdapter mocks an EventHub Target Adapter to return an EventHub config.
func MockEventHubTargetAdapter() target.EventHubTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.EventHubConfig)
		if !ok {
			return nil, errors.New("invalid input, expected EventHubTargetConfig")
		}

		return cfg, nil
	}

}

// MockHTTPTargetAdapter mocks an Http Target Adapter to return an Http config.
func MockHTTPTargetAdapter() target.HTTPTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.HTTPTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected HTTPTargetConfig")
		}

		return cfg, nil
	}

}

// MockKafkaTargetAdapter mocks a Kafka Target Adapter to return a Kafka config.
func MockKafkaTargetAdapter() target.KafkaTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.KafkaConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KafkaTargetConfig")
		}

		return cfg, nil
	}

}

// MockKinesisTargetAdapter mocks a Kinesis Target Adapter to return a Kinesis config.
func MockKinesisTargetAdapter() target.KinesisTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.KinesisTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected KinesisTargetConfig")
		}

		return cfg, nil
	}

}

// MockPubSubTargetAdapter mocks a PubSub Target Adapter to return a PubSub config.
func MockPubSubTargetAdapter() target.PubSubTargetAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*target.PubSubTargetConfig)
		if !ok {
			return nil, errors.New("invalid input, expected PubSubTargetConfig")
		}

		return cfg, nil
	}

}
