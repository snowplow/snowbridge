// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package target

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/pkg/errors"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/twinj/uuid"
)

var cfg = EventHubConfig{
	EventHubNamespace:       "test",
	EventHubName:            "test",
	MaxAutoRetries:          1,
	MessageByteLimit:        1048576,
	ChunkByteLimit:          1048576,
	ChunkMessageLimit:       500,
	ContextTimeoutInSeconds: 20,
	BatchByteLimit:          1048576,
	SetEHPartitionKey:       true,
}

var errMock = errors.New("Mock Failure Path")

type mockHub struct {
	// Channel to output results
	results chan *eventhub.EventBatch
	// Boolean to allow us to mock failure path
	fail bool
}

// Sendbatch is a mock of the Eventhubs SendBatch method. If m.fail is true, it returns an error.
// Otherwise, it uses the provided BatchIterator to mimic the batching behaviour in the client, and feeds
// those batches into the m.results channel.
func (m mockHub) SendBatch(ctx context.Context, iterator eventhub.BatchIterator, opts ...eventhub.BatchOption) error {
	if m.fail {
		return errMock
	}

	//mimic eventhubs SendBatch behaviour loosely
	batchOptions := &eventhub.BatchOptions{
		MaxSize: eventhub.DefaultMaxMessageSizeInBytes,
	}

	for _, opt := range opts {
		if err := opt(batchOptions); err != nil {

			return err
		}
	}

	for !iterator.Done() {
		id := uuid.NewV4()

		batch, err := iterator.Next(id.String(), batchOptions)
		if err != nil {
			return err
		}
		m.results <- batch
	}
	return nil
}

// Close isn't used, it's just here to satisfy the mock API interface
func (m mockHub) Close(context.Context) error {
	return nil
}

// getResults retrieves and returns results from the mock's results channel,
// it blocks until no result have come in for the timeout period
func getResults(resultChannel chan *eventhub.EventBatch, timeout time.Duration) []*eventhub.EventBatch {
	res := make([]*eventhub.EventBatch, 0)

ResultsLoop:
	for {
		select {
		case batch := <-resultChannel:
			res = append(res, batch)
		case <-time.After(1 * time.Second):
			break ResultsLoop
		}
	}

	return res
}

// TestProcessWithRandomPartitionKeys tests the process() function happy path when we set the eventhub partition key to a random value.
// When we explicitly set the partition key, events are batched by partition key - so random PK should result in batches of 1.
func TestProcessWithRandomPartitionKeys(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := newEventHubTargetWithInterfaces(m, &cfg)

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, testutil.GenRandomString(100), ackFunc)

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.process(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amonut of batches
	assert.Equal(10, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(10), ackOps)
	// Check that we got no error and the TargetWriteResult is as expected.
	assert.Nil(err)
	assert.Equal(10, len(twres.Sent))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestProcessFailure tests that we get correct behaviour in a failure scenario.
func TestProcessFailure(t *testing.T) {
	assert := assert.New(t)

	// Unhappy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
		fail:    true,
	}
	tgtToFail := newEventHubTargetWithInterfaces(m, &cfg)

	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, testutil.GenRandomString(100), ackFunc)

	var twres *models.TargetWriteResult
	var err error

	// We don't really need to spawn a goroutine here,
	// however not doing so and reading results will make the test hang when misconfigured
	// so for future debuggers' sanity let's do it this way.
	go func() {
		twres, err = tgtToFail.process(messages)
	}()

	failRes := getResults(m.results, 500*time.Millisecond)

	// Check that we got correct amonut of batches
	assert.Equal(0, len(failRes))
	// Check that we acked correct amount of times
	assert.Equal(int64(0), ackOps)
	// Check that we got the desired error and the TargetWriteResult is as expected.
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to send message batch to EventHub: Mock Failure Path", err.Error())
	}
	assert.Nil(twres.Sent)
	assert.Equal(10, len(twres.Failed))
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestProcessWithNoPartitionKey tests the process() function happy path when we don't set a partition key.
func TestProcessWithNoPartitionKey(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := newEventHubTargetWithInterfaces(m, &cfg)
	tgt.setEHPartitionKey = false

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, testutil.GenRandomString(100), ackFunc)

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.process(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amonut of batches
	assert.Equal(1, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(10), ackOps)
	// Check that we got no error and the TargetWriteResult is as expected.
	assert.Nil(err)
	assert.Equal(10, len(twres.Sent))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestProcessBatchingByPartitionKey tests that the process function batches per partition key as expected.
func TestProcessBatchingByPartitionKey(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := newEventHubTargetWithInterfaces(m, &cfg)

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(99, testutil.GenRandomString(100), ackFunc)

	// Assign one of three evenly distributed partition keys
	for i, msg := range messages {
		msg.PartitionKey = fmt.Sprintf("PK%d", i%3)
	}

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.process(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amonut of batches
	assert.Equal(3, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(99), ackOps)
	// Check that we got no error and the TargetWriteResult is as expected.
	assert.Nil(err)
	assert.Equal(99, len(twres.Sent))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)

	// The data iteslf isn't public from the EH client, but at least we can check that the partition keys are as expected.
	pksFound := make([]string, 0)
	for _, r := range res {
		pksFound = append(pksFound, *r.Event.PartitionKey)
	}
	sort.Strings(pksFound)
	assert.Equal([]string{"PK0", "PK1", "PK2"}, pksFound)
}

// TestWriteSuccess test the happy path for the Write() function.
func TestWriteSuccess(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := newEventHubTargetWithInterfaces(m, &cfg)
	// Max chunk size of 20 just to validate behaviour with some chunking involved.
	tgt.chunkMessageLimit = 20

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(100, testutil.GenRandomString(100), ackFunc)

	// Set the partition key all to the same value to ensure that batching behaviour is down to chunking rather than EH client batching (which we test elsewhere)
	for _, msg := range messages {
		msg.PartitionKey = "testPK"
	}

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.Write(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amonut of batches
	assert.Equal(5, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(100), ackOps)
	// Check that we got no error and the TargetWriteResult is as expected.
	assert.Nil(err)
	assert.Equal(100, len(twres.Sent))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestWriteFailure tests the unhappy path for the Write function.
func TestWriteFailure(t *testing.T) {
	assert := assert.New(t)

	// Unhappy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
		fail:    true,
	}
	tgt := newEventHubTargetWithInterfaces(m, &cfg)
	// Max chunk size of 20 just to validate behaviour with several errors
	tgt.chunkMessageLimit = 20

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(100, testutil.GenRandomString(100), ackFunc)

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.Write(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amonut of batches
	assert.Equal(0, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(0), ackOps)
	// Check that we got the expected error and the TargetWriteResult is as expected.
	assert.NotNil(err)
	if err != nil {
		assert.True(strings.Contains(err.Error(), "Error writing messages to EventHub: 5 errors occurred:"))
		assert.Equal(5, strings.Count(err.Error(), "Failed to send message batch to EventHub: Mock Failure Path"))
	}
	assert.Nil(twres.Sent)
	assert.Equal(100, len(twres.Failed))
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestNewEventHubTarget_KeyValue tests that we can initialise a client with key value credentials.
func TestNewEventHubTarget_KeyValue(t *testing.T) {
	assert := assert.New(t)

	// Test that we can initialise a client with Key and Value
	t.Setenv("EVENTHUB_KEY_NAME", "fake")
	t.Setenv("EVENTHUB_KEY_VALUE", "fake")

	tgt, err := newEventHubTarget(&cfg)
	assert.Nil(err)
	assert.NotNil(tgt)
}

// TestNewEventHubTarget_ConnString tests that we can initialise a client with connection string credentials.
func TestNewEventHubTarget_ConnString(t *testing.T) {
	assert := assert.New(t)

	// Test that we can initialise a client with Connection String

	t.Setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=fake;SharedAccessKey=fake")

	tgt, err := newEventHubTarget(&cfg)
	assert.Nil(err)
	assert.NotNil(tgt)
}

// TestNewEventHubTarget_CredentialsNotFound tests that we fail on startup when we're not provided with appropriate credential values.
func TestNewEventHubTarget_CredentialsNotFound(t *testing.T) {
	assert := assert.New(t)

	tgt, err := newEventHubTarget(&cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment", err.Error())
	}
	assert.Nil(tgt)
}

// NewEventHubTarget should fail if we can't reach EventHub, commented out this test until we look into https://github.com/snowplow-devops/stream-replicator/issues/151
// Note that when we do so, the above tests will need to be changed to use some kind of mock
/*
func TestNewEventHubTarget_Failure(t *testing.T) {
	assert := assert.New(t)

	// Test that we can initialise a client with Key and Value
	t.Setenv("EVENTHUB_KEY_NAME", "fake")
	t.Setenv("EVENTHUB_KEY_VALUE", "fake")

	tgt, err := newEventHubTarget(&cfg)
	assert.Equal("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment", err.Error())
	assert.Nil(tgt)
}
*/
