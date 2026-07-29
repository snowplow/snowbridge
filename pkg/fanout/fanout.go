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

// Package fanout coordinates the ack/nack of one parent message across
// the N child messages it gets fanned out to.
package fanout

import (
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/snowplow/snowbridge/v6/pkg/models"
)

// CreateChild builds a child *models.Message carrying data, pre-wired so
// its AckFunc/NackFunc feed the parent's resolution.
type CreateChild func(data []byte) *models.Message

// Run invokes fn with a CreateChild bound to parent. The parent resolves
// exactly once:
//   - parent.AckFunc fires once fn has returned and every child created
//     has acked. With no children, that's the moment fn returns.
//   - parent.NackFunc fires on the first child nack; subsequent ack/nack
//     calls on any child no-op.
//
// If fn panics, neither fires — the parent is left unresolved so the
// source's inflight machinery redelivers after restart.
func Run(parent *models.Message, fn func(createChild CreateChild)) {
	f := &fanOut{parent: parent}
	f.pending.Store(1)
	fn(f.createChild)
	// Not deferred: a panic in fn must leave the parent unresolved so the
	// source redelivers. Deferring here would ack a panicked parent.
	f.doAck()
}

// fanOut tracks pending children and a single resolution latch.
type fanOut struct {
	parent   *models.Message
	pending  atomic.Int64
	resolved atomic.Bool
}

func (f *fanOut) createChild(data []byte) *models.Message {
	f.pending.Add(1)
	// One Once shared between ack and nack: the first call on this child
	// wins, repeats are no-ops. Prevents a buggy double-ack from leaking
	// an extra decrement and resolving the parent while siblings pend.
	var once sync.Once
	return &models.Message{
		Data:         data,
		OriginalData: data,
		PartitionKey: uuid.NewString(),
		TimeCreated:  f.parent.TimeCreated,
		TimePulled:   f.parent.TimePulled,
		AckFunc:      func() { once.Do(f.doAck) },
		NackFunc:     func() { once.Do(f.doNack) },
	}
}

func (f *fanOut) doAck() {
	if f.pending.Add(-1) == 0 && f.resolved.CompareAndSwap(false, true) {
		if f.parent.AckFunc != nil {
			f.parent.AckFunc()
		}
	}
}

func (f *fanOut) doNack() {
	// Fire parent.NackFunc on the first nack, regardless of pending siblings.
	// Subsequent acks and the trailing doAck see resolved=true and no-op.
	if f.resolved.CompareAndSwap(false, true) {
		if f.parent.NackFunc != nil {
			f.parent.NackFunc()
		}
	}
	f.pending.Add(-1)
}
