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

package kinesis

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v5/pkg/models"
	"github.com/snowplow/snowbridge/v5/pkg/testutil"
)

// statefulMockKinesisClient supports multi-call scenarios where the first N calls
// return throttle errors and subsequent calls succeed.
type statefulMockKinesisClient struct {
	callCount int64
	// throttleOnCalls is the number of calls that should return throttle errors.
	// Once callCount exceeds this, PutRecords returns success.
	throttleOnCalls int64
	// nonThrottleErrCode, if non-empty, returns that error code for each record (not throttle).
	nonThrottleErrCode string
	// callLevelErr, if non-nil, is returned as the PutRecords error (not per-record).
	callLevelErr error
}

func (m *statefulMockKinesisClient) PutRecords(ctx context.Context, input *kinesis.PutRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	call := atomic.AddInt64(&m.callCount, 1)

	if m.callLevelErr != nil {
		return nil, m.callLevelErr
	}

	throttleErrCode := provisionedThroughputExceededException.ErrorCode()
	throttleMsg := "Rate exceeded for shard"

	if m.nonThrottleErrCode != "" {
		// Return a non-throttle per-record error for every record
		records := make([]types.PutRecordsResultEntry, len(input.Records))
		for i := range records {
			code := m.nonThrottleErrCode
			msg := "record-level error"
			records[i] = types.PutRecordsResultEntry{
				ErrorCode:    &code,
				ErrorMessage: &msg,
			}
		}
		return &kinesis.PutRecordsOutput{Records: records}, nil
	}

	if call <= m.throttleOnCalls {
		// Return throttle for all records
		records := make([]types.PutRecordsResultEntry, len(input.Records))
		for i := range records {
			records[i] = types.PutRecordsResultEntry{
				ErrorCode:    &throttleErrCode,
				ErrorMessage: &throttleMsg,
			}
		}
		return &kinesis.PutRecordsOutput{Records: records}, nil
	}

	// Return success for all records
	records := make([]types.PutRecordsResultEntry, len(input.Records))
	for i, entry := range input.Records {
		records[i] = types.PutRecordsResultEntry{
			SequenceNumber: aws.String("seq-" + *entry.PartitionKey),
			ShardId:        aws.String("shardId-000000000000"),
		}
	}
	return &kinesis.PutRecordsOutput{Records: records}, nil
}

func (m *statefulMockKinesisClient) CreateStream(ctx context.Context, input *kinesis.CreateStreamInput, opts ...func(*kinesis.Options)) (*kinesis.CreateStreamOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) DeleteStream(ctx context.Context, input *kinesis.DeleteStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DeleteStreamOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) DescribeStream(ctx context.Context, input *kinesis.DescribeStreamInput, opts ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) ListShards(ctx context.Context, input *kinesis.ListShardsInput, opts ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) GetRecords(ctx context.Context, input *kinesis.GetRecordsInput, opts ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) PutRecord(ctx context.Context, input *kinesis.PutRecordInput, opts ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) MergeShards(ctx context.Context, input *kinesis.MergeShardsInput, opts ...func(*kinesis.Options)) (*kinesis.MergeShardsOutput, error) {
	return nil, nil
}
func (m *statefulMockKinesisClient) GetShardIterator(ctx context.Context, input *kinesis.GetShardIteratorInput, opts ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	return nil, nil
}

// TestKinesisWrite_ThrottleHandledInternally confirms that ProvisionedThroughputExceededException
// errors are retried inside Write() without surfacing to the router.
// The router must never see a ThrottleWriteError from Kinesis.
func TestKinesisWrite_ThrottleHandledInternally(t *testing.T) {
	assert := assert.New(t)

	client := &statefulMockKinesisClient{throttleOnCalls: 1}

	target, err := newKinesisTargetWithInterfaces(client, "000000000000", "us-east-1", "test-stream", 500)
	assert.Nil(err)

	var ackOps int64
	ackFunc := func() { atomic.AddInt64(&ackOps, 1) }

	messages := testutil.GetTestMessages(3, "Hello Kinesis!!", ackFunc)
	writeRes, writeErr := target.Write(messages)

	// Write must succeed after internal retry — no error reaches the router
	assert.Nil(writeErr)
	assert.NotNil(writeRes)

	// All messages must be acked
	assert.Equal(int64(3), ackOps)

	// All messages in Sent, none in Failed
	assert.Equal(3, len(writeRes.Sent))
	assert.Equal(0, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Invalid))

	// PutRecords was called twice: once returning throttle, once succeeding
	assert.Equal(int64(2), atomic.LoadInt64(&client.callCount))

	// Error must NOT be wrapped as ThrottleWriteError or any model error type
	if writeErr != nil {
		_, isThrottle := writeErr.(models.ThrottleWriteError)
		assert.False(isThrottle, "Kinesis must never surface ThrottleWriteError to the router")
	}
}

// TestKinesisWrite_NonThrottleRecordError confirms that non-throttle per-record errors
// are returned as a plain error (transient), not as SetupWriteError/ThrottleWriteError/FatalWriteError.
func TestKinesisWrite_NonThrottleRecordError(t *testing.T) {
	assert := assert.New(t)

	client := &statefulMockKinesisClient{nonThrottleErrCode: "InternalFailure"}

	target, err := newKinesisTargetWithInterfaces(client, "000000000000", "us-east-1", "test-stream", 500)
	assert.Nil(err)

	messages := testutil.GetTestMessages(3, "Hello Kinesis!!", nil)
	writeRes, writeErr := target.Write(messages)

	// Error must be present
	assert.NotNil(writeErr)

	// Error must be a plain error — not any of the special model error types
	_, isSetup := writeErr.(models.SetupWriteError)
	_, isThrottle := writeErr.(models.ThrottleWriteError)
	_, isFatal := writeErr.(models.FatalWriteError)
	assert.False(isSetup, "non-throttle Kinesis errors must not be SetupWriteError")
	assert.False(isThrottle, "non-throttle Kinesis errors must not be ThrottleWriteError")
	assert.False(isFatal, "non-throttle Kinesis errors must not be FatalWriteError")

	// Failed messages must be returned for the router's transient retry
	assert.Equal(3, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Sent))
}

// TestKinesisWrite_BatchCallError confirms that a call-level PutRecords error (e.g. network failure)
// is returned as a plain error with all messages in Failed for the router's transient retry.
func TestKinesisWrite_BatchCallError(t *testing.T) {
	assert := assert.New(t)

	client := &statefulMockKinesisClient{callLevelErr: errors.New("connection refused")}

	target, err := newKinesisTargetWithInterfaces(client, "000000000000", "us-east-1", "test-stream", 500)
	assert.Nil(err)

	messages := testutil.GetTestMessages(3, "Hello Kinesis!!", nil)
	writeRes, writeErr := target.Write(messages)

	assert.NotNil(writeErr)

	// Error must be plain (transient) — not any special error type
	_, isSetup := writeErr.(models.SetupWriteError)
	_, isThrottle := writeErr.(models.ThrottleWriteError)
	_, isFatal := writeErr.(models.FatalWriteError)
	assert.False(isSetup)
	assert.False(isThrottle)
	assert.False(isFatal)

	// All messages must be in Failed for router retry
	assert.Equal(3, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Sent))
}
