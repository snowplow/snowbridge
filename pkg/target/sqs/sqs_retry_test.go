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

package sqs

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v5/pkg/models"
	"github.com/snowplow/snowbridge/v5/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/v5/pkg/testutil"
)

// mockSQSClient implements common.SqsV2API for unit testing.
// sendMessageBatchOutput is returned for SendMessageBatch; all other methods are no-ops.
type mockSQSClient struct {
	sendMessageBatchOutput *sqs.SendMessageBatchOutput
	sendMessageBatchErr    error
}

func (m *mockSQSClient) SendMessageBatch(ctx context.Context, input *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return m.sendMessageBatchOutput, m.sendMessageBatchErr
}

func (m *mockSQSClient) ChangeMessageVisibility(ctx context.Context, input *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return nil, nil
}
func (m *mockSQSClient) CreateQueue(ctx context.Context, input *sqs.CreateQueueInput, opts ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	return nil, nil
}
func (m *mockSQSClient) DeleteMessage(ctx context.Context, input *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return nil, nil
}
func (m *mockSQSClient) DeleteQueue(ctx context.Context, input *sqs.DeleteQueueInput, opts ...func(*sqs.Options)) (*sqs.DeleteQueueOutput, error) {
	return nil, nil
}
func (m *mockSQSClient) GetQueueUrl(ctx context.Context, input *sqs.GetQueueUrlInput, opts ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return nil, nil
}
func (m *mockSQSClient) ReceiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return nil, nil
}
func (m *mockSQSClient) SendMessage(ctx context.Context, input *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	return nil, nil
}

// newSQSTargetDriverWithMock creates an SQSTargetDriver with a mocked client for unit testing.
func newSQSTargetDriverWithMock(client *mockSQSClient) *SQSTargetDriver {
	return &SQSTargetDriver{
		BatchingConfig: targetiface.BatchingConfig{
			MaxBatchMessages:     sqsSendMessageBatchChunkSize,
			MaxBatchBytes:        sqsSendMessageBatchByteLimit,
			MaxMessageBytes:      sqsSendMessageByteLimit,
			MaxConcurrentBatches: 5,
			FlushPeriodMillis:    200,
		},
		client:    client,
		queueURL:  "https://sqs.us-east-1.amazonaws.com/000000000000/test-queue",
		queueName: "test-queue",
		region:    "us-east-1",
		accountID: "000000000000",
		log:       log.WithFields(log.Fields{"target": SupportedTargetSQS}),
	}
}

// TestSQSWrite_InvalidMessageContentsRoutedToInvalid confirms that messages rejected with
// InvalidMessageContents are placed in the Invalid list and not retried by the router.
func TestSQSWrite_InvalidMessageContentsRoutedToInvalid(t *testing.T) {
	assert := assert.New(t)

	invalidCode := invalidMsgContents.ErrorCode()
	invalidMsg := "Message body must be valid UTF-8"

	client := &mockSQSClient{
		sendMessageBatchOutput: &sqs.SendMessageBatchOutput{
			Failed: []sqstypes.BatchResultErrorEntry{
				{
					Id:          aws.String("0"),
					Code:        aws.String(invalidCode),
					Message:     aws.String(invalidMsg),
					SenderFault: true,
				},
			},
			Successful: []sqstypes.SendMessageBatchResultEntry{
				{Id: aws.String("1"), MessageId: aws.String("msg-id-1")},
				{Id: aws.String("2"), MessageId: aws.String("msg-id-2")},
			},
		},
	}

	target := newSQSTargetDriverWithMock(client)
	messages := testutil.GetTestMessages(3, "test payload", nil)
	writeRes, writeErr := target.Write(messages)

	// The invalid message must not generate a retryable error
	assert.Nil(writeErr)
	assert.NotNil(writeRes)

	// InvalidMessageContents must go to Invalid, not Failed
	assert.Equal(1, len(writeRes.Invalid), "InvalidMessageContents message must be in Invalid list")
	assert.Equal(0, len(writeRes.Failed), "InvalidMessageContents message must not be in Failed list")
	assert.Equal(2, len(writeRes.Sent))
}

// TestSQSWrite_OtherMessageFailureReturnsTransient confirms that non-InvalidMessageContents
// per-message errors are placed in Failed (not Invalid) and returned as a plain error,
// signalling the router to apply transient retry.
func TestSQSWrite_OtherMessageFailureReturnsTransient(t *testing.T) {
	assert := assert.New(t)

	retryableCode := "ServiceUnavailable"

	client := &mockSQSClient{
		sendMessageBatchOutput: &sqs.SendMessageBatchOutput{
			Failed: []sqstypes.BatchResultErrorEntry{
				{
					Id:          aws.String("0"),
					Code:        aws.String(retryableCode),
					Message:     aws.String("Service is temporarily unavailable"),
					SenderFault: false,
				},
			},
			Successful: []sqstypes.SendMessageBatchResultEntry{},
		},
	}

	target := newSQSTargetDriverWithMock(client)
	messages := testutil.GetTestMessages(1, "test payload", nil)
	writeRes, writeErr := target.Write(messages)

	// A retryable error must be present for the router to trigger transient retry
	assert.NotNil(writeErr)

	// Error must be plain (transient) — not any special model error type
	_, isSetup := writeErr.(models.SetupWriteError)
	_, isThrottle := writeErr.(models.ThrottleWriteError)
	_, isFatal := writeErr.(models.FatalWriteError)
	assert.False(isSetup, "SQS retry errors must not be SetupWriteError")
	assert.False(isThrottle, "SQS retry errors must not be ThrottleWriteError")
	assert.False(isFatal, "SQS retry errors must not be FatalWriteError")

	// Failed message must be in Failed list for router retry
	assert.Equal(1, len(writeRes.Failed))
	assert.Equal(0, len(writeRes.Invalid))
}
