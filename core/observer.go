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

type Observer struct {
	stopSignal      chan struct{}
	targetWriteChan chan *WriteResult
	timeout         time.Duration
	reportInterval  time.Duration
}

func NewObserver(timeout time.Duration, reportInterval time.Duration) *Observer {
	return &Observer{
		stopSignal:      make(chan struct{}),
		targetWriteChan: make(chan *WriteResult, 1000),
		timeout:         timeout,
		reportInterval:  reportInterval,
	}
}

func (o *Observer) Start() {
	go func() {
		reportTime := time.Now().Add(o.reportInterval)

		sent := int64(0)
		failed := int64(0)
		total := int64(0)

		for {
			select {
			case <-o.stopSignal:
				log.Debugf("Observer received stop signal, closing ...")
				break
			case res := <-o.targetWriteChan:
				if res != nil {
					sent += res.Sent
					failed += res.Failed
					total += res.Total()
				}
			case <-time.After(o.timeout):
				log.Debugf("Observer timed out waiting (%v) for result", o.timeout)
			}

			if time.Now().After(reportTime) {
				log.Infof("Observer report - Sent: %d, Failed: %d, Total: %d", sent, failed, total)

				sent = int64(0)
				failed = int64(0)
				total = int64(0)

				reportTime = time.Now().Add(o.reportInterval)
			}
		}
	}()
}

func (o *Observer) Stop() {
	o.stopSignal <- struct{}{}
}

// --- Functions called to push information to observer

// TargetWrite pushes a targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWrite(r *WriteResult) {
	o.targetWriteChan <- r
}
