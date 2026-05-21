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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v5/pkg/models"
	"github.com/snowplow/snowbridge/v5/pkg/monitoring"
	"github.com/snowplow/snowbridge/v5/pkg/statsreceiver/statsreceiveriface"
)

// Observer holds the channels and settings for aggregating telemetry from processed messages
// and emitting them to downstream destinations
type Observer struct {
	statsClient            statsreceiveriface.StatsReceiver
	errorsMetadataClient   monitoring.MetadataReporterer
	exitSignal             chan struct{}
	wg                     sync.WaitGroup
	filteredChan           chan *models.TargetWriteResult
	targetWriteChan        chan *models.TargetWriteResult
	targetWriteInvalidChan chan *models.TargetWriteResult
	reportInterval         time.Duration
	isRunning              bool

	// Kinsumer metrics channels
	kinsumerRecordsChan      chan int64
	kinsumerRecordsBytesChan chan int64

	metadataChan chan *bufferSnapshot

	log *log.Entry
}

// bufferSnapshot is the unit of ownership transferred from the ingestion loop to the flush loop.
type bufferSnapshot struct {
	buffer *models.ObserverBuffer
	start  time.Time
	end    time.Time
}

// New builds a new observer to be used to gather telemetry
// about target writes
func New(statsClient statsreceiveriface.StatsReceiver, reportInterval time.Duration, metadataClient monitoring.MetadataReporterer) *Observer {
	return &Observer{
		statsClient:              statsClient,
		errorsMetadataClient:     metadataClient,
		exitSignal:               make(chan struct{}),
		filteredChan:             make(chan *models.TargetWriteResult, 1000),
		targetWriteChan:          make(chan *models.TargetWriteResult, 1000),
		targetWriteInvalidChan:   make(chan *models.TargetWriteResult, 1000),
		kinsumerRecordsChan:      make(chan int64, 1000),
		kinsumerRecordsBytesChan: make(chan int64, 1000),
		reportInterval:           reportInterval,
		log:                      log.WithFields(log.Fields{"name": "Observer"}),
		isRunning:                false,
	}
}

// Start launches the ingestion and flush goroutines.
func (o *Observer) Start() {
	if o.isRunning {
		o.log.Warn("Observer is already running")
		return
	}
	o.isRunning = true
	// Cap 2: one slot for a regular tick snapshot, one for a possible exit-signal
	// follow-up — so the final window still lands even if metadataLoop is mid-Send.
	o.metadataChan = make(chan *bufferSnapshot, 2)

	// Ingestion hands work to metadataLoop via forwardToMetadata() — never blocks on it.
	o.wg.Go(o.ingestionLoop)
	o.wg.Go(o.metadataLoop)
}

func (o *Observer) ingestionLoop() {
	periodStart := time.Now().UTC()
	current := newBuffer(0, 0)

	ticker := time.NewTicker(o.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-o.exitSignal:
			end := time.Now().UTC()
			snapshot := &bufferSnapshot{buffer: current, start: periodStart, end: end}
			o.publishStats(snapshot.buffer)
			o.forwardToMetadata(snapshot)
			close(o.metadataChan)
			return
		case res := <-o.filteredChan:
			current.AppendFiltered(res)
		case res := <-o.targetWriteChan:
			current.AppendWrite(res)
		case res := <-o.targetWriteInvalidChan:
			current.AppendWriteInvalid(res)
		case count := <-o.kinsumerRecordsChan:
			current.KinsumerRecordsInMemory = count
		case bytes := <-o.kinsumerRecordsBytesChan:
			current.KinsumerRecordsInMemoryBytes = bytes
		case <-ticker.C:
			end := time.Now().UTC()
			snapshot := &bufferSnapshot{buffer: current, start: periodStart, end: end}
			// Gauges represent current state, not period counts — carry them over.
			current = newBuffer(current.KinsumerRecordsInMemory, current.KinsumerRecordsInMemoryBytes)
			periodStart = end
			o.publishStats(snapshot.buffer)
			o.forwardToMetadata(snapshot)
		}
	}
}

func (o *Observer) metadataLoop() {
	for snap := range o.metadataChan {
		if o.errorsMetadataClient != nil {
			o.errorsMetadataClient.Send(snap.buffer, snap.start, snap.end)
		}
	}
}

func (o *Observer) publishStats(buffer *models.ObserverBuffer) {
	o.log.Info(buffer.String())
	if o.statsClient != nil {
		o.statsClient.Send(buffer)
	}
}

func (o *Observer) forwardToMetadata(snapshot *bufferSnapshot) {
	select {
	case o.metadataChan <- snapshot:
	default:
		o.log.Warnf(
			"metadata loop busy; dropping snapshot for window %s -> %s (%s)",
			snapshot.start.Format(time.RFC3339),
			snapshot.end.Format(time.RFC3339),
			snapshot.buffer.String(),
		)
	}
}

func newBuffer(recordsInMemory, recordsInMemoryBytes int64) *models.ObserverBuffer {
	return &models.ObserverBuffer{
		InvalidErrors:                make(map[models.MetadataCodeDescription]int),
		FailedErrors:                 make(map[models.MetadataCodeDescription]int),
		KinsumerRecordsInMemory:      recordsInMemory,
		KinsumerRecordsInMemoryBytes: recordsInMemoryBytes,
	}
}

// Stop issues a signal to halt observer processing
func (o *Observer) Stop() {
	o.log.Info("Observer Stop() called")
	if o.isRunning {
		o.exitSignal <- struct{}{}
		o.wg.Wait()
		o.isRunning = false
	}
}

// --- Functions called to push information to observer

// TargetWrite pushes normal targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWrite(r *models.TargetWriteResult) {
	o.targetWriteChan <- r
}

// TargetWriteInvalid pushes an invalid targets write result onto a channel for processing
// by the observer
func (o *Observer) TargetWriteInvalid(r *models.TargetWriteResult) {
	o.targetWriteInvalidChan <- r
}

func (o *Observer) TargetWriteFiltered(r *models.TargetWriteResult) {
	o.filteredChan <- r
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
