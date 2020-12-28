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
	targetWriteChan chan *WriteResult
	timeout         time.Duration
	reportInterval  time.Duration
	log             *log.Entry
}

// NewObserver builds a new observer to be used to gather telemetry
// about target writes
func NewObserver(timeout time.Duration, reportInterval time.Duration) *Observer {
	return &Observer{
		stopSignal:      make(chan struct{}),
		targetWriteChan: make(chan *WriteResult, 1000),
		timeout:         timeout,
		reportInterval:  reportInterval,
		log:             log.WithFields(log.Fields{"name": "Observer"}),
	}
}

// Start launches a goroutine which processes results from target writes
// TODO: Prevent starting multiple background processors
func (o *Observer) Start() {
	go func() {
		reportTime := time.Now().Add(o.reportInterval)

		sent := int64(0)
		failed := int64(0)
		total := int64(0)

		for {
			select {
			case <-o.stopSignal:
				o.log.Debugf("Received stop signal, closing ...")
				break
			case res := <-o.targetWriteChan:
				if res != nil {
					sent += res.Sent
					failed += res.Failed
					total += res.Total()
				}
			case <-time.After(o.timeout):
				o.log.Warnf("Timed out after (%v) waiting for result", o.timeout)
			}

			if time.Now().After(reportTime) {
				o.log.Infof("Sent:%d,Failed:%d,Total:%d", sent, failed, total)

				sent = int64(0)
				failed = int64(0)
				total = int64(0)

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
func (o *Observer) TargetWrite(r *WriteResult) {
	o.targetWriteChan <- r
}
