// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package statsreceiver

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	statsd "github.com/smira/go-statsd"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// StatsDStatsReceiverConfig configures the stats metrics receiver
type StatsDStatsReceiverConfig struct {
	Address string `hcl:"address,optional" env:"STATS_RECEIVER_STATSD_ADDRESS"`
	Prefix  string `hcl:"prefix,optional" env:"STATS_RECEIVER_STATSD_PREFIX"`
	Tags    string `hcl:"tags,optional" env:"STATS_RECEIVER_STATSD_TAGS"`
}

// statsDStatsReceiver holds a new client for writing statistics to a StatsD server
type statsDStatsReceiver struct {
	client *statsd.Client
}

// newStatsDStatsReceiver creates a new client for writing metrics to StatsD
func newStatsDStatsReceiver(address string, prefix string, tagsRaw string, tagsMapClient map[string]string) (*statsDStatsReceiver, error) {
	tagsMap := map[string]string{}
	err := json.Unmarshal([]byte(tagsRaw), &tagsMap)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshall STATSD_TAGS to map")
	}

	var tags []statsd.Tag
	for key, value := range tagsMap {
		tags = append(tags, statsd.StringTag(key, value))
	}
	for key, value := range tagsMapClient {
		tags = append(tags, statsd.StringTag(key, value))
	}

	client := statsd.NewClient(address,
		statsd.MaxPacketSize(1400),
		statsd.MetricPrefix(fmt.Sprintf("%s.", prefix)),
		statsd.TagStyle(statsd.TagFormatDatadog),
		statsd.DefaultTags(tags...),
		statsd.ReconnectInterval(60*time.Second),
	)

	return &statsDStatsReceiver{
		client: client,
	}, nil
}

// NewStatsDReceiverWithTags closes over a given tags map and returns a function
// that creates a statsDStatsReceiver given a StatsDStatsReceiverConfig.
func NewStatsDReceiverWithTags(tags map[string]string) func(c *StatsDStatsReceiverConfig) (*statsDStatsReceiver, error) {
	return func(c *StatsDStatsReceiverConfig) (*statsDStatsReceiver, error) {
		return newStatsDStatsReceiver(
			c.Address,
			c.Prefix,
			c.Tags,
			tags,
		)
	}
}

// The StatsDStatsReceiverAdapter type is an adapter for functions to be used as
// pluggable components for StatsD Stats Receiver.
// It implements the Pluggable interface.
type StatsDStatsReceiverAdapter func(i interface{}) (interface{}, error)

// Create implements the ComponentCreator interface.
func (f StatsDStatsReceiverAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f StatsDStatsReceiverAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &StatsDStatsReceiverConfig{
		Prefix: "snowplow.stream-replicator",
		Tags:   "{}",
	}

	return cfg, nil
}

// AdaptStatsDStatsReceiverFunc returns a StatsDStatsReceiverAdapter.
func AdaptStatsDStatsReceiverFunc(f func(c *StatsDStatsReceiverConfig) (*statsDStatsReceiver, error)) StatsDStatsReceiverAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*StatsDStatsReceiverConfig)
		if !ok {
			return nil, errors.New("invalid input, expected StatsDStatsReceiverConfig")
		}

		return f(cfg)
	}
}

// Send emits the bufferred metrics to the receiver
func (s *statsDStatsReceiver) Send(b *models.ObserverBuffer) {
	s.client.Incr("message_sent", b.MsgSent)
	s.client.Incr("message_failed", b.MsgFailed)
	s.client.Incr("oversized_message_sent", b.OversizedMsgSent)
	s.client.Incr("oversized_message_failed", b.OversizedMsgFailed)
	s.client.Incr("invalid_message_sent", b.InvalidMsgSent)
	s.client.Incr("invalid_message_failed", b.InvalidMsgFailed)
	s.client.PrecisionTiming("latency_processing_max", b.MaxProcLatency)
	s.client.PrecisionTiming("latency_message_max", b.MaxMsgLatency)
}
