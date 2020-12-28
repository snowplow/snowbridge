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

		msgSent := int64(0)
		msgFailed := int64(0)
		msgTotal := int64(0)
		maxProcLatency := time.Duration(0)
		minProcLatency := time.Duration(0)
		avgProcLatency := time.Duration(0)
		sumProcLatency := time.Duration(0)
		maxMessageLatency := time.Duration(0)
		minMessageLatency := time.Duration(0)
		avgMessageLatency := time.Duration(0)
		sumMessageLatency := time.Duration(0)

		for {
			select {
			case <-o.stopSignal:
				o.log.Debugf("Received stop signal, closing ...")
				o.isRunning = false
				break
			case res := <-o.targetWriteChan:
				if res != nil {
					msgSent += res.Sent
					msgFailed += res.Failed
					msgTotal += res.Total()

					if maxProcLatency < res.MaxProcLatency {
						maxProcLatency = res.MaxProcLatency
					}
					if minProcLatency > res.MinProcLatency {
						minProcLatency = res.MinProcLatency
					}
					sumProcLatency += res.AvgProcLatency

					if maxMessageLatency < res.MaxMessageLatency {
						maxMessageLatency = res.MaxMessageLatency
					}
					if minMessageLatency > res.MinMessageLatency {
						minMessageLatency = res.MinMessageLatency
					}
					sumMessageLatency += res.AvgMessageLatency
				}
			case <-time.After(o.timeout):
				o.log.Debugf("Timed out after (%v) waiting for result", o.timeout)
			}

			if time.Now().After(reportTime) {
				if msgTotal > 0 {
					avgProcLatency = time.Duration(int64(sumProcLatency)/msgTotal) * time.Nanosecond
					avgMessageLatency = time.Duration(int64(sumMessageLatency)/msgTotal) * time.Nanosecond
				}

				o.log.Infof(
					"Sent:%d,Failed:%d,Total:%d,MaxProcLatency:%s,MinProcLatency:%s,AvgProcLatency:%s,MaxMessageLatency:%s,MinMessageLatency:%s,AvgMessageLatency:%s",
					msgSent,
					msgFailed,
					msgTotal,
					maxProcLatency,
					minProcLatency,
					avgProcLatency,
					maxMessageLatency,
					minMessageLatency,
					avgMessageLatency,
				)

				msgSent = int64(0)
				msgFailed = int64(0)
				msgTotal = int64(0)
				maxProcLatency = time.Duration(0)
				minProcLatency = time.Duration(0)
				avgProcLatency = time.Duration(0)
				sumProcLatency = time.Duration(0)
				maxMessageLatency = time.Duration(0)
				minMessageLatency = time.Duration(0)
				avgMessageLatency = time.Duration(0)
				sumMessageLatency = time.Duration(0)

				reportTime = time.Now().Add(o.reportInterval)
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
