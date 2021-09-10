// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package statsreceiver

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"
	statsd "github.com/smira/go-statsd"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// StatsDStatsReceiver holds a new client for writing statistics to a StatsD server
type StatsDStatsReceiver struct {
	client *statsd.Client
}

// NewStatsDStatsReceiver creates a new client for writing metrics to StatsD
func NewStatsDStatsReceiver(address string, prefix string, tagsRaw string, tagsMapClient map[string]string) (*StatsDStatsReceiver, error) {
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

	return &StatsDStatsReceiver{
		client: client,
	}, nil
}

// Send emits the bufferred metrics to the receiver
func (s *StatsDStatsReceiver) Send(b *models.ObserverBuffer) {
	s.client.Incr("message_sent", b.MsgSent)
	s.client.Incr("message_failed", b.MsgFailed)
	s.client.Incr("oversized_message_sent", b.OversizedMsgSent)
	s.client.Incr("oversized_message_failed", b.OversizedMsgFailed)
	s.client.Incr("invalid_message_sent", b.InvalidMsgSent)
	s.client.Incr("invalid_message_failed", b.InvalidMsgFailed)
	s.client.PrecisionTiming("latency_proccesing_max", b.MaxProcLatency)
	s.client.PrecisionTiming("latency_message_max", b.MaxMsgLatency)
}
