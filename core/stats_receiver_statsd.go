// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/cactus/go-statsd-client/v4/statsd"
	log "github.com/sirupsen/logrus"
)

// StatsDStatsReceiver holds a new client for writing statistics to a StatsD server
type StatsDStatsReceiver struct {
	client statsd.StatSender
	log    *log.Entry
}

// NewStdoutTarget creates a new client for writing messages to stdout
func NewStatsDStatsReceiver(address string, prefix string) (*StatsDStatsReceiver, error) {
	sd, err := statsd.NewClientWithConfig(&statsd.ClientConfig{
		Address: address,
		Prefix:  prefix,
	})
	if err != nil {
		return nil, err
	}

	return &StatsDStatsReceiver{
		client: sd,
		log:    log.WithFields(log.Fields{"name": "StatsDStatsReceiver"}),
	}, nil
}

// Send emits the bufferred metrics to the receiver
func (s *StatsDStatsReceiver) Send(b *ObserverBuffer) {
	_ = s.client.Gauge("target_results", b.TargetResults, 1.0)
	_ = s.client.Inc("msg_sent", b.MsgSent, 1.0)
	_ = s.client.Inc("msg_failed", b.MsgFailed, 1.0)
	_ = s.client.Inc("msg_total", b.MsgTotal, 1.0)
	_ = s.client.TimingDuration("max_proc_latency", b.MaxProcLatency, 1.0)
	_ = s.client.TimingDuration("min_proc_latency", b.MinProcLatency, 1.0)
	_ = s.client.TimingDuration("avg_proc_latency", b.GetAvgProcLatency(), 1.0)
	_ = s.client.TimingDuration("max_msg_latency", b.MaxMsgLatency, 1.0)
	_ = s.client.TimingDuration("min_msg_latency", b.MinMsgLatency, 1.0)
	_ = s.client.TimingDuration("avg_msg_latency", b.GetAvgMsgLatency(), 1.0)
}
