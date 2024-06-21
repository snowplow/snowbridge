/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package statsreceiver

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	statsd "github.com/smira/go-statsd"

	"github.com/snowplow/snowbridge/pkg/models"
)

// StatsDStatsReceiverConfig configures the stats metrics receiver
type StatsDStatsReceiverConfig struct {
	Address string `hcl:"address,optional"`
	Prefix  string `hcl:"prefix,optional"`
	Tags    string `hcl:"tags,optional"`
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
		Prefix: "snowplow.snowbridge",
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
	// overall
	s.client.Incr("target_success", b.MsgSent)
	s.client.Incr("target_failed", b.MsgFailed)
	s.client.Incr("message_filtered", b.MsgFiltered)

	// unsendable
	s.client.Incr("failure_target_success", b.OversizedMsgSent+b.InvalidMsgSent)
	s.client.Incr("failure_target_failed", b.OversizedMsgFailed+b.InvalidMsgFailed)

	// latencies
	s.client.PrecisionTiming("min_processing_latency", b.MinProcLatency)
	s.client.PrecisionTiming("max_processing_latency", b.MaxProcLatency)
	s.client.PrecisionTiming("avg_processing_latency", b.GetAvgProcLatency())

	s.client.PrecisionTiming("min_message_latency", b.MinMsgLatency)
	s.client.PrecisionTiming("max_message_latency", b.MaxMsgLatency)
	s.client.PrecisionTiming("avg_message_latency", b.GetAvgMsgLatency())

	s.client.PrecisionTiming("min_transform_latency", b.MinTransformLatency)
	s.client.PrecisionTiming("max_transform_latency", b.MaxTransformLatency)
	s.client.PrecisionTiming("avg_transform_latency", b.GetAvgTransformLatency())

	s.client.PrecisionTiming("min_filter_latency", b.MinFilterLatency)
	s.client.PrecisionTiming("max_filter_latency", b.MaxFilterLatency)
	s.client.PrecisionTiming("avg_filter_latency", b.GetAvgFilterLatency())

	s.client.PrecisionTiming("min_request_latency", b.MinRequestLatency)
	s.client.PrecisionTiming("max_request_latency", b.MaxRequestLatency)
	s.client.PrecisionTiming("avg_request_latency", b.GetAvgRequestLatency())
}
