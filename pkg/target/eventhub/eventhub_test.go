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

package eventhub

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
)

var cfg = EventHubConfig{
	BatchingConfig: &targetiface.BatchingConfig{
		MaxBatchMessages:     500,
		MaxBatchBytes:        1048576,
		MaxMessageBytes:      1048576,
		MaxConcurrentBatches: 5,
		FlushPeriodMillis:    500,
	},
	EventHubNamespace:       "test",
	EventHubName:            "test",
	MaxAutoRetries:          1,
	ContextTimeoutInSeconds: 20,
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
		id := uuid.New()

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
		case <-time.After(timeout):
			break ResultsLoop
		}
	}

	return res
}

func TestEventHubTargetDriver_Batcher(t *testing.T) {
	driver := &EventHubTargetDriver{}
	defaultConfig := driver.GetDefaultConfiguration().(*EventHubConfig)
	driver.BatchingConfig = *defaultConfig.BatchingConfig

	// Test 1: Adding one message to a batch with 499 messages should trigger send
	// Create a current batch with 499 small messages
	smallMessages := testutil.GetTestMessages(499, "small", nil)
	currentBatchDataBytes := 0
	for _, msg := range smallMessages {
		currentBatchDataBytes += len(msg.Data)
	}

	currentBatch := targetiface.CurrentBatch{
		Messages:  smallMessages,
		DataBytes: currentBatchDataBytes,
	}

	// Add one more small message (the 500th)
	additionalMessage := testutil.GetTestMessages(1, "small", nil)[0]

	batchToSend, newCurrentBatch, oversized := driver.Batcher(currentBatch, additionalMessage)

	// Verify complete batch is sent (500 messages - EventHub's max)
	assert.Len(t, batchToSend, 500, "Should send complete batch of 500 messages")

	// Verify new current batch is empty
	assert.Len(t, newCurrentBatch.Messages, 0, "Should have empty current batch after sending")
	assert.Equal(t, 0, newCurrentBatch.DataBytes, "Should have 0 bytes in new current batch")

	// Verify no oversized message
	assert.Nil(t, oversized, "Should have no oversized message")

	// Test 2: Oversized message should be returned as oversized
	// Create an oversized message (larger than 1MB)
	oversizedMessage := testutil.GetTestMessages(1, testutil.GenRandomString(1100000), nil)[0]

	// Start with empty batch for oversized test
	emptyBatch := targetiface.CurrentBatch{}

	batchToSend2, newCurrentBatch2, oversized2 := driver.Batcher(emptyBatch, oversizedMessage)

	// Verify no batch is sent
	assert.Nil(t, batchToSend2, "Should not send any batch for oversized message")

	// Verify current batch remains empty
	assert.Len(t, newCurrentBatch2.Messages, 0, "Current batch should remain empty")
	assert.Equal(t, 0, newCurrentBatch2.DataBytes, "Current batch bytes should remain 0")

	// Verify oversized message is returned
	assert.NotNil(t, oversized2, "Should return oversized message")
	assert.Equal(t, oversizedMessage, oversized2, "Should return the exact oversized message")
}

// TestWriteWithRandomPartitionKeys tests the Write() function happy path when we set the eventhub partition key to a random value.
// When we explicitly set the partition key, events are batched by partition key - so random PK should result in batches of 1.
func TestWriteWithRandomPartitionKeys(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := &EventHubTargetDriver{}
	if err := tgt.newEventHubTargetDriverWithInterfaces(m, &cfg); err != nil {
		t.Fatalf("failed to create eventhub target driver: %s", err)
	}

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(10, testutil.GenRandomString(100), ackFunc)

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.Write(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amount of batches
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

// TestWriteFailure tests that we get correct behaviour in a failure scenario.
func TestWriteFailure(t *testing.T) {
	assert := assert.New(t)

	// Unhappy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
		fail:    true,
	}
	tgtToFail := &EventHubTargetDriver{}
	if err := tgtToFail.newEventHubTargetDriverWithInterfaces(m, &cfg); err != nil {
		t.Fatalf("failed to create eventhub target driver: %s", err)
	}

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
		twres, err = tgtToFail.Write(messages)
	}()

	failRes := getResults(m.results, 500*time.Millisecond)

	// Check that we got correct amount of batches
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

// TestWriteWithNoPartitionKey tests the Write() function happy path when we don't set a partition key.
func TestWriteWithNoPartitionKey(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := &EventHubTargetDriver{}
	if err := tgt.newEventHubTargetDriverWithInterfaces(m, &cfg); err != nil {
		t.Fatalf("failed to create eventhub target driver: %s", err)
	}
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
		twres, err = tgt.Write(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amount of batches
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

// TestWriteBatchingByPartitionKey tests that the Write function batches per partition key as expected.
func TestWriteBatchingByPartitionKey(t *testing.T) {
	assert := assert.New(t)

	// Happy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
	}
	tgt := &EventHubTargetDriver{}
	if err := tgt.newEventHubTargetDriverWithInterfaces(m, &cfg); err != nil {
		t.Fatalf("failed to create eventhub target driver: %s", err)
	}

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
		twres, err = tgt.Write(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amount of batches
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
		pksFound = append(pksFound, *r.PartitionKey)
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
	tgt := &EventHubTargetDriver{}
	if err := tgt.newEventHubTargetDriverWithInterfaces(m, &cfg); err != nil {
		t.Fatalf("failed to create eventhub target driver: %s", err)
	}

	// Mechanism for counting acks
	var ackOps int64
	ackFunc := func() {
		atomic.AddInt64(&ackOps, 1)
	}

	messages := testutil.GetTestMessages(100, testutil.GenRandomString(100), ackFunc)

	// Set the partition key all to the same value to ensure that batching behaviour is managed by EventHub client
	for _, msg := range messages {
		msg.PartitionKey = "testPK"
	}

	var twres *models.TargetWriteResult
	var err error

	go func() {
		twres, err = tgt.Write(messages)
	}()
	res := getResults(m.results, 1*time.Second)

	// Check that we got correct amount of batches (with same partition key, should be 1 batch)
	assert.Equal(1, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(100), ackOps)
	// Check that we got no error and the TargetWriteResult is as expected.
	assert.Nil(err)
	assert.Equal(100, len(twres.Sent))
	assert.Nil(twres.Failed)
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestWriteFailureNew tests the unhappy path for the Write function.
func TestWriteFailureNew(t *testing.T) {
	assert := assert.New(t)

	// Unhappy path
	m := mockHub{
		results: make(chan *eventhub.EventBatch),
		fail:    true,
	}
	tgt := &EventHubTargetDriver{}
	if err := tgt.newEventHubTargetDriverWithInterfaces(m, &cfg); err != nil {
		t.Fatalf("failed to create eventhub target driver: %s", err)
	}

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

	// Check that we got correct amount of batches
	assert.Equal(0, len(res))
	// Check that we acked correct amount of times
	assert.Equal(int64(0), ackOps)
	// Check that we got the expected error and the TargetWriteResult is as expected.
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Failed to send message batch to EventHub: Mock Failure Path", err.Error())
	}
	assert.Nil(twres.Sent)
	assert.Equal(100, len(twres.Failed))
	assert.Nil(twres.Oversized)
	assert.Nil(twres.Invalid)
}

// TestNewEventHubTargetDriver_KeyValue tests that we can initialise a client with key value credentials.
func TestNewEventHubTargetDriver_KeyValue(t *testing.T) {
	assert := assert.New(t)

	// Test that we can initialise a client with Key and Value
	t.Setenv("EVENTHUB_KEY_NAME", "fake")
	t.Setenv("EVENTHUB_KEY_VALUE", "fake")

	tgt := &EventHubTargetDriver{}
	err := tgt.InitFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(tgt)
}

// TestNewEventHubTargetDriver_ConnString tests that we can initialise a client with connection string credentials.
func TestNewEventHubTargetDriver_ConnString(t *testing.T) {
	assert := assert.New(t)

	// Test that we can initialise a client with Connection String

	t.Setenv("EVENTHUB_CONNECTION_STRING", "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=fake;SharedAccessKey=fake")

	tgt := &EventHubTargetDriver{}
	err := tgt.InitFromConfig(&cfg)
	assert.Nil(err)
	assert.NotNil(tgt)
}

// TestNewEventHubTargetDriver_CredentialsNotFound tests that we fail on startup when we're not provided with appropriate credential values.
func TestNewEventHubTargetDriver_CredentialsNotFound(t *testing.T) {
	assert := assert.New(t)

	tgt := &EventHubTargetDriver{}
	err := tgt.InitFromConfig(&cfg)
	assert.NotNil(err)
	if err != nil {
		assert.Equal("Error initialising EventHub client: No valid combination of authentication Env vars found. https://pkg.go.dev/github.com/Azure/azure-event-hubs-go#NewHubWithNamespaceNameAndEnvironment", err.Error())
	}
}

// NewEventHubTarget should fail if we can't reach EventHub, commented out this test until we look into https://github.com/snowplow/snowbridge/issues/151
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
