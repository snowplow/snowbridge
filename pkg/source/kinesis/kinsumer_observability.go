package kinesissource

import (
	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/observer"
	"time"
)

type kinsumerLogrus struct{}

// Log will print all Kinsumer logs as DEBUG lines
func (kl *kinsumerLogrus) Log(format string, v ...any) {
	log.WithFields(log.Fields{"source": "KinesisSource.Kinsumer"}).Debugf(format, v...)
}

// kinsumerStatsWrapper wraps an observer to implement kinsumer's stats interface
type kinsumerStatsWrapper struct {
	observer *observer.Observer
}

// newKinsumerStatsWrapper creates a wrapper that provides kinsumer-specific metrics
func newKinsumerStatsWrapper(obs *observer.Observer) *kinsumerStatsWrapper {
	return &kinsumerStatsWrapper{
		observer: obs,
	}
}

// Kinsumer stats interface implementation
func (w *kinsumerStatsWrapper) Checkpoint() {
	// No-op: not necessary for now
}

func (w *kinsumerStatsWrapper) EventToClient(inserted, retrieved time.Time) {
	// No-op: not necessary for now
}

func (w *kinsumerStatsWrapper) EventsFromKinesis(num int, shardID string, lag time.Duration) {
	// No-op: not necessary for now
}

func (w *kinsumerStatsWrapper) RecordsInMemory(count int64) {
	if w.observer != nil {
		w.observer.UpdateKinsumerRecordsInMemory(count)
	}
}

func (w *kinsumerStatsWrapper) RecordsInMemoryBytes(bytes int64) {
	if w.observer != nil {
		w.observer.UpdateKinsumerRecordsInMemoryBytes(bytes)
	}
}
