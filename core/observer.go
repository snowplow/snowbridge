// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	log "github.com/sirupsen/logrus"
	"time"
)

// Observer holds the channels and settings for aggregating telemetry events
// and emitting them to downstream destinations
type Observer struct {
	stopSignal      chan struct{}
	targetWriteChan chan *TargetWriteResult
	timeout         time.Duration
	reportInterval  time.Duration
	log             *log.Entry
	isRunning       bool
}

// NewObserver builds a new observer to be used to gather telemetry
// about target writes
func NewObserver(timeout time.Duration, reportInterval time.Duration) *Observer {
	return &Observer{
		stopSignal:      make(chan struct{}),
		targetWriteChan: make(chan *TargetWriteResult, 1000),
		timeout:         timeout,
		reportInterval:  reportInterval,
		log:             log.WithFields(log.Fields{"name": "Observer"}),
		isRunning:       false,
	}
}

// Start launches a goroutine which processes results from target writes
func (o *Observer) Start() {
	if o.isRunning {
		o.log.Warn("Observer is already running")
	}
	o.isRunning = true

	go func() {
		reportTime := time.Now().Add(o.reportInterval)
		buffer := MetricsBuffer{}

		for {
			select {
			case <-o.stopSignal:
				o.log.Debugf("Received stop signal, closing ...")
				o.isRunning = false
				break
			case res := <-o.targetWriteChan:
				buffer.Append(res)
			case <-time.After(o.timeout):
				o.log.Debugf("Timed out after (%v) waiting for result", o.timeout)
			}

			if time.Now().After(reportTime) {
				avgProcLatency := buffer.GetAvgProcLatency()
				avgMessageLatency := buffer.GetAvgMessageLatency()

				o.log.Infof(
					"Sent:%d,Failed:%d,Total:%d,MaxProcLatency:%s,MinProcLatency:%s,AvgProcLatency:%s,MaxMessageLatency:%s,MinMessageLatency:%s,AvgMessageLatency:%s",
					buffer.MsgSent,
					buffer.MsgFailed,
					buffer.MsgTotal,
					buffer.MaxProcLatency,
					buffer.MinProcLatency,
					avgProcLatency,
					buffer.MaxMessageLatency,
					buffer.MinMessageLatency,
					avgMessageLatency,
				)

				reportTime = time.Now().Add(o.reportInterval)
				buffer = MetricsBuffer{}
			}
		}
	}()
}

// Stop issues a signal to halt observer processing
func (o *Observer) Stop() {
	o.stopSignal <- struct{}{}
}

// --- Functions called to push information to observer

// TargetWrite pushes a targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWrite(r *TargetWriteResult) {
	o.targetWriteChan <- r
}
