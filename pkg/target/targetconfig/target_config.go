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

package targetconfig

import (
	"fmt"
	"sync"
	"time"

	config "github.com/snowplow/snowbridge/v3/config"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/eventhub"
	"github.com/snowplow/snowbridge/v3/pkg/target/http"
	"github.com/snowplow/snowbridge/v3/pkg/target/kafka"
	"github.com/snowplow/snowbridge/v3/pkg/target/kinesis"
	"github.com/snowplow/snowbridge/v3/pkg/target/pubsub"
	"github.com/snowplow/snowbridge/v3/pkg/target/silent"
	"github.com/snowplow/snowbridge/v3/pkg/target/sqs"
	"github.com/snowplow/snowbridge/v3/pkg/target/stdout"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
)

// GetTarget creates and returns the target that is configured.
func GetTarget(targetCfg *config.TargetConfig, decoder config.Decoder) (*targetiface.Target, error) {
	useTarget := targetCfg.Target
	decoderOpts := &config.DecoderOptions{
		Input: useTarget.Body,
	}

	var driver targetiface.TargetDriver
	var err error

	switch useTarget.Name {
	case stdout.SupportedTargetStdout:
		driver = &stdout.StdoutTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*stdout.StdoutTargetConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}
		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}
		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case kafka.SupportedTargetKafka:
		driver = &kafka.KafkaTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*kafka.KafkaConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case pubsub.SupportedTargetPubsub:
		driver = &pubsub.PubSubTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*pubsub.PubSubTargetConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case kinesis.SupportedTargetKinesis:
		driver = &kinesis.KinesisTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*kinesis.KinesisTargetConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case http.SupportedTargetHTTP:
		driver = &http.HTTPTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*http.HTTPTargetConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case sqs.SupportedTargetSQS:
		driver = &sqs.SQSTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*sqs.SQSTargetConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case eventhub.SupportedTargetEventHub:
		driver = &eventhub.EventHubTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*eventhub.EventHubConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	case silent.SupportedTargetSilent:
		driver = &silent.SilentTargetDriver{}

		c := driver.GetDefaultConfiguration()
		cfg, ok := c.(*silent.SilentTargetConfig)
		if !ok {
			return nil, fmt.Errorf("invalid configuration type")
		}

		if err := decoder.Decode(decoderOpts, cfg); err != nil {
			return nil, err
		}

		err = driver.InitFromConfig(cfg)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown target: %s", useTarget.Name)
	}

	batchingConfig := driver.GetBatchingConfig()

	tickerPeriod := time.Duration(batchingConfig.FlushPeriodMillis) * time.Millisecond
	ticker := time.NewTicker(tickerPeriod)

	// Wrap driver in Target with batching configuration
	return &targetiface.Target{
		TargetDriver: driver,
		CurrentBatch: targetiface.CurrentBatch{Messages: []*models.Message{}, DataBytes: 0},
		WaitGroup:    &sync.WaitGroup{},
		Throttle:     make(chan struct{}, batchingConfig.MaxConcurrentBatches),
		Ticker:       ticker,
		TickerPeriod: tickerPeriod,
	}, nil
}
