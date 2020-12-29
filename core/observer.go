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

// Observer holds the channels and settings for aggregating telemetry from processed messages
// and emitting them to downstream destinations
type Observer struct {
	statsClient     StatsReceiver
	exitSignal      chan struct{}
	stopDone        chan struct{}
	targetWriteChan chan *TargetWriteResult
	timeout         time.Duration
	reportInterval  time.Duration
	log             *log.Entry
	isRunning       bool
}

// NewObserver builds a new observer to be used to gather telemetry
// about target writes
func NewObserver(statsClient StatsReceiver, timeout time.Duration, reportInterval time.Duration) *Observer {
	return &Observer{
		statsClient:     statsClient,
		exitSignal:      make(chan struct{}),
		stopDone:        make(chan struct{}),
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
		return
	}
	o.isRunning = true

	go func() {
		reportTime := time.Now().Add(o.reportInterval)
		buffer := ObserverBuffer{}

	ObserverLoop:
		for {
			select {
			case <-o.exitSignal:
				o.log.Warn("Received exit signal, shutting down Observer ...")

				// Attempt final flush
				o.log.Infof(buffer.String())
				if o.statsClient != nil {
					o.statsClient.Send(&buffer)
				}

				o.isRunning = false
				break ObserverLoop
			case res := <-o.targetWriteChan:
				buffer.Append(res)
			case <-time.After(o.timeout):
				o.log.Debugf("Timed out after (%v) waiting for result", o.timeout)
			}

			if time.Now().After(reportTime) {
				o.log.Infof(buffer.String())
				if o.statsClient != nil {
					o.statsClient.Send(&buffer)
				}

				reportTime = time.Now().Add(o.reportInterval)
				buffer = ObserverBuffer{}
			}
		}
		o.stopDone <- struct{}{}
	}()
}

// Stop issues a signal to halt observer processing
func (o *Observer) Stop() {
	if o.isRunning {
		o.exitSignal <- struct{}{}
		<-o.stopDone
	}
}

// --- Functions called to push information to observer

// TargetWrite pushes a targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWrite(r *TargetWriteResult) {
	o.targetWriteChan <- r
}
