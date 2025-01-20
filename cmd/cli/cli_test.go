/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 */

package cli

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/failure"
	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/observer"
	"github.com/stretchr/testify/assert"
)

func TestWrite_AllOK(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{sent: []string{"m1", "m2"}}, //first write attempt, all good
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToGood)
	assert.Empty(t, output.sentToFailed)
	assert.Empty(t, output.err)
}

func TestWrite_OKAfterFailed(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{failed: []string{"m1", "m2"}, err: "Error 1"}, //first write attempt fails
			{sent: []string{"m1", "m2"}},                   // but second is ok
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToGood)
	assert.Empty(t, output.sentToFailed)
	assert.Empty(t, output.err)
}

func TestWrite_AllInvalid(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{invalid: []string{"m1", "m2"}}, //first write attempt signals that data is invalid
		},
		failureTarget: []mockResult{
			{sent: []string{"m1", "m2"}}, // so it's later successfully sent to the failure target
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToFailed)
	assert.Empty(t, output.sentToGood)
	assert.Empty(t, output.err)
}

func TestWrite_InvalidRetried(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{invalid: []string{"m1", "m2"}}, //first write attempt signals that data is invalid
		},
		failureTarget: []mockResult{
			{failed: []string{"m1", "m2"}, err: "failure target error 1"}, // but first attempt to write invalid data to the failure target fails
			{failed: []string{"m1", "m2"}, err: "failure target error 2"}, //the second one too
			{sent: []string{"m1", "m2"}},                                  // but third one is ok
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToFailed)
	assert.Empty(t, output.sentToGood)
	assert.Empty(t, output.err)
}

func TestWrite_SomeOKSomeInvalid(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{sent: []string{"m1"}, invalid: []string{"m2"}}, //first write attempt turns out to be a mix of valid and invalid data
		},
		failureTarget: []mockResult{
			{sent: []string{"m2"}}, // so invalid part is later then sent to the failure target
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1"}, output.sentToGood) // but good data is in good target
	assert.Equal(t, []string{"m2"}, output.sentToFailed)
	assert.Empty(t, output.err)
}

func TestWrite_OKAfterPartialFailure(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{sent: []string{"m1"}, failed: []string{"m2"}, err: "Error 1"}, // one message is ok, the second one fails
			{failed: []string{"m2"}, err: "Error 2"},                       // so the second one is retried and fails again
			{sent: []string{"m2"}},                                         // but eventually is also successfull
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToGood)
	assert.Empty(t, output.sentToFailed)
	assert.Empty(t, output.err)
}

func TestWrite_AllOversized(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{oversized: []string{"m1", "m2"}},
		},
		failureTarget: []mockResult{
			{sent: []string{"m1", "m2"}},
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToFailed)
	assert.Empty(t, output.sentToGood)
	assert.Empty(t, output.err)
}

func TestWrite_Combo(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
		message("m3", "data 3"),
		message("m4", "data 4"),
	}

	// mix of everything - ok, retrying failures, invalid and oversized messages
	mocks := targetMocks{
		//m1 and m2 are good but m2 fails at first
		goodTarget: []mockResult{
			{sent: []string{"m1"}, failed: []string{"m2"}, oversized: []string{"m3"}, invalid: []string{"m4"}, err: "m2 failed!!!"},
			{failed: []string{"m2"}, err: "m2 failed again!!!"},
			{sent: []string{"m2"}},
		},
		//m3 and m4 are going to bad but with some retries
		failureTarget: []mockResult{
			{failed: []string{"m3"}, err: "m3 (oversized) failed!!"},
			{failed: []string{"m3"}, err: "m3 (oversized) failed!!"},
			{sent: []string{"m3"}},
			{failed: []string{"m4"}, err: "m4 (invalid) failed!!"},
			{failed: []string{"m4"}, err: "m4 (invalid) failed!!"},
			{failed: []string{"m4"}, err: "m4 (invalid) failed!!"},
			{sent: []string{"m4"}},
		},
	}

	output := run(inputMessages, mocks)
	assert.Equal(t, []string{"m1", "m2"}, output.sentToGood)
	assert.Equal(t, []string{"m3", "m4"}, output.sentToFailed)
	assert.Empty(t, output.err)
}

func TestWrite_RunOutOfAttempts(t *testing.T) {
	inputMessages := []*models.Message{
		message("m1", "data 1"),
		message("m2", "data 2"),
	}

	mocks := targetMocks{
		goodTarget: []mockResult{
			{failed: []string{"m1", "m2"}, err: "Error 1"},
			{failed: []string{"m1", "m2"}, err: "Error 2"},
			{failed: []string{"m1", "m2"}, err: "Error 3"},
			{failed: []string{"m1", "m2"}, err: "Error 4"},
			{failed: []string{"m1", "m2"}, err: "Error 5"},
			{failed: []string{"m1", "m2"}, err: "Error 6"},
		},
	}

	output := run(inputMessages, mocks)
	assert.Empty(t, output.sentToGood)
	assert.Empty(t, output.sentToFailed)
	assert.Equal(t, "Error 6", output.err.Error())
}

func run(input []*models.Message, targetMocks targetMocks) testOutput {
	config, _ := config.NewConfig()
	goodTarget := testTarget{results: targetMocks.goodTarget}
	failureTarget := testTarget{results: targetMocks.failureTarget}
	failure, _ := failure.NewSnowplowFailure(&failureTarget, "test-processor", "test-version")
	obs := observer.New(testStatsReceiver{}, time.Minute, time.Second)
	trans := func(m []*models.Message) *models.TransformationResult {
		return models.NewTransformationResult(m, nil, nil)
	}

	f := sourceWriteFunc(&goodTarget, failure, trans, obs, config)

	err := f(input)

	return testOutput{
		sentToGood:   goodTarget.sent,
		sentToFailed: failureTarget.sent,
		err:          err,
	}
}

func (t *testTarget) Write(messages []*models.Message) (*models.TargetWriteResult, error) {
	nextResponse := t.results[t.writesCounter]
	t.writesCounter++

	var err error
	sent := findByKey(nextResponse.sent, messages)
	failed := findByKey(nextResponse.failed, messages)
	invalid := findByKey(nextResponse.invalid, messages)
	oversized := findByKey(nextResponse.oversized, messages)

	if nextResponse.err != "" {
		err = errors.New(nextResponse.err)
	}

	for _, m := range sent {
		t.sent = append(t.sent, m.PartitionKey)
	}

	result := models.NewTargetWriteResult(sent, failed, oversized, invalid)
	return result, err
}

func message(key string, input string) *models.Message {
	return &models.Message{PartitionKey: key, Data: []byte(input)}
}

func findByKey(keys []string, messages []*models.Message) []*models.Message {
	var out []*models.Message

	for _, msg := range messages {
		if slices.Contains(keys, msg.PartitionKey) {
			out = append(out, msg)
		}
	}

	return out
}

type testOutput struct {
	sentToGood   []string
	sentToFailed []string
	err          error
}

type testTarget struct {
	writesCounter int
	results       []mockResult
	sent          []string
}

type targetMocks struct {
	goodTarget    []mockResult
	failureTarget []mockResult
}

type mockResult struct {
	sent      []string
	failed    []string
	invalid   []string
	oversized []string
	err       string
}

func (t *testTarget) Open()  {}
func (t *testTarget) Close() {}
func (t *testTarget) MaximumAllowedMessageSizeBytes() int {
	return 1000
}
func (t *testTarget) GetID() string {
	return "test target"
}

type testStatsReceiver struct {
	stats []*models.ObserverBuffer
}

func (r testStatsReceiver) Send(buffer *models.ObserverBuffer) {
	r.stats = append(r.stats, buffer)
}
