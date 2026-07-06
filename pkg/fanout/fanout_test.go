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

package fanout

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v5/pkg/models"
)

func newCountingParent() (*models.Message, *atomic.Int64, *atomic.Int64) {
	var acks, nacks atomic.Int64
	parent := &models.Message{
		Data:         []byte("parent"),
		OriginalData: []byte("parent"),
		PartitionKey: "parent-key",
		TimeCreated:  time.Unix(1700000000, 0),
		TimePulled:   time.Unix(1700000001, 0),
		AckFunc:      func() { acks.Add(1) },
		NackFunc:     func() { nacks.Add(1) },
	}
	return parent, &acks, &nacks
}

// emit runs Run, accumulates n children inside fn, and returns them unresolved.
func emit(parent *models.Message, n int) []*models.Message {
	var children []*models.Message
	Run(parent, func(createChild CreateChild) {
		for i := 0; i < n; i++ {
			children = append(children, createChild([]byte{byte(i)}))
		}
	})
	return children
}

func TestRun_ParentAcksOnlyAfterLastChildAck(t *testing.T) {
	assert := assert.New(t)
	parent, acks, _ := newCountingParent()
	children := emit(parent, 3)

	children[0].AckFunc()
	assert.EqualValues(0, acks.Load(), "parent must not ack after 1/3 children")
	children[1].AckFunc()
	assert.EqualValues(0, acks.Load(), "parent must not ack after 2/3 children")
	children[2].AckFunc()
	assert.EqualValues(1, acks.Load(), "parent acks exactly when last child acks")
}

func TestRun_AnyChildNacks_NacksParent(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()
	children := emit(parent, 3)

	assert.EqualValues(0, nacks.Load())
	children[1].NackFunc()
	assert.EqualValues(1, nacks.Load())
	assert.EqualValues(0, acks.Load())
}

func TestRun_NackSuppressesSiblingAcks(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()
	children := emit(parent, 3)

	children[0].AckFunc()
	children[1].NackFunc()
	children[2].AckFunc()

	assert.EqualValues(0, acks.Load())
	assert.EqualValues(1, nacks.Load())
}

func TestRun_CreateChildAfterNack_LateAckNoOps(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()

	Run(parent, func(createChild CreateChild) {
		a := createChild([]byte("a"))
		a.NackFunc()
		// Nack already fired and resolved=true. A new child created
		// afterwards must wire cleanly and its ack must be a no-op.
		createChild([]byte("b")).AckFunc()
	})

	assert.EqualValues(0, acks.Load(), "late child ack must not fire parent ack after nack")
	assert.EqualValues(1, nacks.Load())
}

func TestRun_MultipleNacks_FireParentNackOnce(t *testing.T) {
	assert := assert.New(t)
	parent, _, nacks := newCountingParent()
	children := emit(parent, 3)

	for _, c := range children {
		c.NackFunc()
	}
	assert.EqualValues(1, nacks.Load())
}

func TestRun_DoesNotAckUntilFnReturns(t *testing.T) {
	assert := assert.New(t)
	parent, acks, _ := newCountingParent()

	Run(parent, func(createChild CreateChild) {
		createChild([]byte("a")).AckFunc()
		assert.EqualValues(0, acks.Load(), "parent must not ack before fn returns")
	})

	assert.EqualValues(1, acks.Load())
}

func TestRun_NacksImmediatelyDuringFn(t *testing.T) {
	assert := assert.New(t)
	parent, _, nacks := newCountingParent()

	Run(parent, func(createChild CreateChild) {
		a := createChild([]byte("a"))
		createChild([]byte("b")) // never resolved — nack fires regardless
		a.NackFunc()
		assert.EqualValues(1, nacks.Load(), "nack fires while sibling still pending")
	})
}

func TestRun_NoChildren_AcksParent(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()

	Run(parent, func(createChild CreateChild) {})

	assert.EqualValues(1, acks.Load())
	assert.EqualValues(0, nacks.Load())
}

func TestRun_PanicInFn_LeavesParentUnresolved(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()

	assert.PanicsWithValue("boom", func() {
		Run(parent, func(createChild CreateChild) {
			panic("boom")
		})
	})

	assert.EqualValues(0, acks.Load(), "panic must not ack")
	assert.EqualValues(0, nacks.Load())
}

func TestRun_NilParentAckFunc_NoPanic(t *testing.T) {
	assert := assert.New(t)
	parent := &models.Message{}

	assert.NotPanics(func() {
		Run(parent, func(createChild CreateChild) {
			createChild([]byte("a")).AckFunc()
		})
	})
}

func TestRun_NilParentNackFunc_NoPanic(t *testing.T) {
	assert := assert.New(t)
	parent := &models.Message{}

	assert.NotPanics(func() {
		Run(parent, func(createChild CreateChild) {
			createChild([]byte("a")).NackFunc()
		})
	})
}

func TestRun_ChildInheritsParentTimestampsAndData(t *testing.T) {
	assert := assert.New(t)
	parent, _, _ := newCountingParent()

	var child *models.Message
	Run(parent, func(createChild CreateChild) {
		child = createChild([]byte("payload"))
	})

	assert.Equal([]byte("payload"), child.Data)
	assert.Equal([]byte("payload"), child.OriginalData)
	assert.Equal(parent.TimeCreated, child.TimeCreated)
	assert.Equal(parent.TimePulled, child.TimePulled)
	assert.NotEmpty(child.PartitionKey)
	assert.NotNil(child.AckFunc)
	assert.NotNil(child.NackFunc)
}

func TestRun_ChildrenHaveUniquePartitionKeys(t *testing.T) {
	assert := assert.New(t)
	parent, _, _ := newCountingParent()
	children := emit(parent, 10)

	keys := map[string]struct{}{}
	for _, c := range children {
		keys[c.PartitionKey] = struct{}{}
	}
	assert.Len(keys, 10)
}

func TestRun_ConcurrentAcks_AcksParentOnce(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()
	children := emit(parent, 100)

	var wg sync.WaitGroup
	for _, c := range children {
		wg.Go(func() {
			c.AckFunc()
		})
	}
	wg.Wait()

	assert.EqualValues(1, acks.Load())
	assert.EqualValues(0, nacks.Load())
}

func TestRun_ConcurrentCreateChild_AcksParentOnce(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()

	Run(parent, func(createChild CreateChild) {
		var wg sync.WaitGroup
		for i := range 100 {
			wg.Go(func() {
				createChild([]byte{byte(i)}).AckFunc()
			})
		}
		wg.Wait()
	})

	assert.EqualValues(1, acks.Load())
	assert.EqualValues(0, nacks.Load())
}

func TestRun_DoubleAckFromOneChild_DoesNotAckParentWhileSiblingPending(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()
	children := emit(parent, 2)

	// Buggy caller acks the same child twice. Child 1 never resolves.
	// Parent must not ack — a sibling is still in flight.
	children[0].AckFunc()
	children[0].AckFunc()

	assert.EqualValues(0, acks.Load(), "double-ack on one child must not ack parent while sibling pending")
	assert.EqualValues(0, nacks.Load())
}

func TestRun_ConcurrentAckAndNack_FiresExactlyOneResolution(t *testing.T) {
	assert := assert.New(t)
	parent, acks, nacks := newCountingParent()
	children := emit(parent, 50)

	var wg sync.WaitGroup
	for i, c := range children {
		wg.Go(func() {
			if i%2 == 0 {
				c.AckFunc()
			} else {
				c.NackFunc()
			}
		})
	}
	wg.Wait()

	assert.EqualValues(0, acks.Load(), "nack always wins when both ack and nack are in the mix")
	assert.EqualValues(1, nacks.Load(), "parent nacks exactly once")
}
