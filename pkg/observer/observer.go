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

package observer

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/monitoring"
	"github.com/snowplow/snowbridge/v3/pkg/statsreceiver/statsreceiveriface"
)

// Observer holds the channels and settings for aggregating telemetry from processed messages
// and emitting them to downstream destinations
type Observer struct {
	statsClient              statsreceiveriface.StatsReceiver
	errorsMetadataClient     monitoring.MetadataReporterer
	exitSignal               chan struct{}
	stopDone                 chan struct{}
	filteredChan             chan *models.FilterResult
	targetWriteChan          chan *models.TargetWriteResult
	targetWriteOversizedChan chan *models.TargetWriteResult
	targetWriteInvalidChan   chan *models.TargetWriteResult
	timeout                  time.Duration
	reportInterval           time.Duration
	isRunning                bool

	// Kinsumer metrics channels
	kinsumerRecordsChan      chan int64
	kinsumerRecordsBytesChan chan int64

	log *log.Entry
}

// New builds a new observer to be used to gather telemetry
// about target writes
func New(statsClient statsreceiveriface.StatsReceiver, timeout, reportInterval time.Duration, metadataClient monitoring.MetadataReporterer) *Observer {
	return &Observer{
		statsClient:              statsClient,
		errorsMetadataClient:     metadataClient,
		exitSignal:               make(chan struct{}),
		stopDone:                 make(chan struct{}),
		filteredChan:             make(chan *models.FilterResult, 1000),
		targetWriteChan:          make(chan *models.TargetWriteResult, 1000),
		targetWriteOversizedChan: make(chan *models.TargetWriteResult, 1000),
		targetWriteInvalidChan:   make(chan *models.TargetWriteResult, 1000),
		kinsumerRecordsChan:      make(chan int64, 1000),
		kinsumerRecordsBytesChan: make(chan int64, 1000),
		timeout:                  timeout,
		reportInterval:           reportInterval,
		log:                      log.WithFields(log.Fields{"name": "Observer"}),
		isRunning:                false,
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
		reportTime := time.Now().UTC().Add(o.reportInterval)
		buffer := models.ObserverBuffer{
			InvalidErrors: make(map[models.MetadataCodeDescription]int),
			FailedErrors:  make(map[models.MetadataCodeDescription]int),
		}

	ObserverLoop:
		for {
			select {
			case <-o.exitSignal:
				o.log.Warn("Received exit signal, shutting down Observer ...")

				// Attempt final flush
				o.flushStats(&buffer, reportTime)

				o.isRunning = false
				break ObserverLoop
			case res := <-o.filteredChan:
				buffer.AppendFiltered(res)
			case res := <-o.targetWriteChan:
				buffer.AppendWrite(res)
			case res := <-o.targetWriteOversizedChan:
				buffer.AppendWriteOversized(res)
			case res := <-o.targetWriteInvalidChan:
				buffer.AppendWriteInvalid(res)
			case count := <-o.kinsumerRecordsChan:
				buffer.KinsumerRecordsInMemory = count
			case bytes := <-o.kinsumerRecordsBytesChan:
				buffer.KinsumerRecordsInMemoryBytes = bytes
			case <-time.After(o.timeout):
				o.log.Debugf("Observer timed out after (%v) waiting for result", o.timeout)
			}

			// We can make separate report time/buffers for errors metadata
			if time.Now().UTC().After(reportTime) {
				o.flushStats(&buffer, reportTime)

				reportTime = time.Now().UTC().Add(o.reportInterval)
				buffer = models.ObserverBuffer{
					InvalidErrors: make(map[models.MetadataCodeDescription]int),
					FailedErrors:  make(map[models.MetadataCodeDescription]int),

					KinsumerRecordsInMemory:      buffer.KinsumerRecordsInMemory,
					KinsumerRecordsInMemoryBytes: buffer.KinsumerRecordsInMemoryBytes,
				}
			}
		}
		o.stopDone <- struct{}{}
	}()
}

func (o *Observer) flushStats(buffer *models.ObserverBuffer, reportTime time.Time) {
	o.log.Info(buffer.String())
	if o.statsClient != nil {
		o.statsClient.Send(buffer)
	}

	if o.errorsMetadataClient != nil {
		o.errorsMetadataClient.Send(buffer, reportTime.Add(-o.reportInterval), time.Now().UTC())
	}
}

// Stop issues a signal to halt observer processing
func (o *Observer) Stop() {
	o.log.Info("Observer Stop() called")
	if o.isRunning {
		o.exitSignal <- struct{}{}
		<-o.stopDone
	}
}

// --- Functions called to push information to observer

// Filtered pushes a filter result onto a channel for processing
// by the observer
func (o *Observer) Filtered(r *models.FilterResult) {
	o.filteredChan <- r
}

// TargetWrite pushes normal targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWrite(r *models.TargetWriteResult) {
	o.targetWriteChan <- r
}

// TargetWriteOversized pushes an oversized targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWriteOversized(r *models.TargetWriteResult) {
	o.targetWriteOversizedChan <- r
}

// TargetWriteInvalid pushes an invalid targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWriteInvalid(r *models.TargetWriteResult) {
	o.targetWriteInvalidChan <- r
}

// UpdateKinsumerRecordsInMemory updates the current count of records in memory
func (o *Observer) UpdateKinsumerRecordsInMemory(count int64) {
	select {
	case o.kinsumerRecordsChan <- count:
	default:
		// Channel full, skip
		log.Warn("KinsumerRecordsInMemory channel full, metric dropped")
	}
}

// UpdateKinsumerRecordsInMemoryBytes updates the current bytes of records in memory
func (o *Observer) UpdateKinsumerRecordsInMemoryBytes(bytes int64) {
	select {
	case o.kinsumerRecordsBytesChan <- bytes:
	default:
		// Channel full, skip
		log.Warn("KinsumerRecordsInMemoryBytes channel full, metric dropped")
	}
}
