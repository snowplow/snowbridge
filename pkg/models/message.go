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

package models

import (
	"fmt"
	"time"
)

type ErrorType string

const (
	ErrorTypeAPI            ErrorType = "api"
	ErrorTypeTransformation ErrorType = "transformation"
	ErrorTypeTemplate       ErrorType = "template"
)

// Message holds the structure of a generic message to be sent to a target
type Message struct {
	PartitionKey string
	OriginalData []byte
	Data         []byte
	HTTPHeaders  map[string]string

	// CollectorTstamp is the timestamp created by the Snowplow collector, extracted from the `collector_tstamp` atomic field. Used to measure E2E latency
	CollectorTstamp time.Time

	// TimeCreated is when the message was created originally
	TimeCreated time.Time

	// TimePulled is when the message was pulled from the source
	TimePulled time.Time

	// TimeTransformed is when the message has completed its last successful transform function
	TimeTransformed time.Time

	// Time the request began, to measure request latency for debugging purposes
	TimeRequestStarted time.Time

	// Time the request was done, to measure request latency for debugging purposes - we manually track this timestamp unlike other metrics, to get as accurate as possible a picture of just the request latency.
	TimeRequestFinished time.Time

	// AckFunc must be called on a successful message emission to ensure
	// any cleanup process for the source is actioned
	AckFunc func()

	// If the message is invalid it can be decorated with an error
	// message for logging and reporting
	err error

	// If the message is invalid it can be decorated with an errorType
	// for reporting purposes
	errorType ErrorType

	// If the message is invalid it can be decorated with an errorCode
	// for reporting purposes
	errorCode string
}

// SetError sets the value of the message error in case of invalidation
func (m *Message) SetError(err error) {
	m.err = err
}

// GetError returns the error that has been set
func (m *Message) GetError() error {
	return m.err
}

// SetErrorType sets the value of the message error type in case of invalidation
func (m *Message) SetErrorType(eType ErrorType) {
	m.errorType = eType
}

// GetErrorType returns the error type that has been set
func (m *Message) GetErrorType() string {
	return string(m.errorType)
}

// SetErrorCode sets the value of the message error code in case of invalidation
func (m *Message) SetErrorCode(eCode string) {
	m.errorCode = eCode
}

// GetErrorCode returns the error code that has been set
func (m *Message) GetErrorCode() string {
	return m.errorCode
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"PartitionKey:%s,TimeCreated:%v,TimePulled:%v,TimeTransformed:%v,Data:%s",
		m.PartitionKey,
		m.TimeCreated,
		m.TimePulled,
		m.TimeTransformed,
		string(m.Data),
	)
}

// GetChunkedMessages returns an array of chunked message arrays from the original slice
// by taking into account three variables:
//
// 1. How many messages can be in a chunk
// 2. How big any individual event can be (in bytes)
// 3. How many bytes can be in a chunk
func GetChunkedMessages(messages []*Message, chunkSize int, maxMessageByteSize int, maxChunkByteSize int) (divided [][]*Message, oversized []*Message) {
	var chunkBuffer []*Message
	var chunkBufferByteLen int

	for _, msg := range messages {
		msgByteLen := len(msg.Data)

		if msgByteLen > maxMessageByteSize {
			oversized = append(oversized, msg)
		} else if len(chunkBuffer) == chunkSize || (chunkBufferByteLen > 0 && chunkBufferByteLen+msgByteLen > maxChunkByteSize) {
			divided = append(divided, chunkBuffer)

			chunkBuffer = []*Message{msg}
			chunkBufferByteLen = msgByteLen
		} else {
			chunkBuffer = append(chunkBuffer, msg)
			chunkBufferByteLen += msgByteLen
		}
	}

	if len(chunkBuffer) > 0 {
		divided = append(divided, chunkBuffer)
	}
	return divided, oversized
}

// FilterOversizedMessages will filter out all messages that exceed the byte size limit
func FilterOversizedMessages(messages []*Message, maxMessageByteSize int) (safe []*Message, oversized []*Message) {
	for _, msg := range messages {
		msgByteLen := len(msg.Data)

		if msgByteLen > maxMessageByteSize {
			oversized = append(oversized, msg)
		} else {
			safe = append(safe, msg)
		}
	}
	return safe, oversized
}
