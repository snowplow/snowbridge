//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package models

import (
	"fmt"
	"time"
)

// Metadata holds the structure of a message's metadata
type Metadata struct {
	AsString string
	Actual   map[string]interface{}
}

// Message holds the structure of a generic message to be sent to a target
type Message struct {
	PartitionKey string
	Data         []byte
	Metadata     *Metadata

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
}

// SetError sets the value of the message error in case of invalidation
func (m *Message) SetError(err error) {
	m.err = err
}

// GetError returns the error that has been set
func (m *Message) GetError() error {
	return m.err
}

// GetActual returns the message's Actual Metadata
func (meta *Metadata) GetActual() map[string]interface{} {
	if meta == nil {
		return nil
	}
	return meta.Actual
}

// GetString returns the message's Metadata AsString
func (meta *Metadata) GetString() string {
	if meta == nil {
		return ""
	}
	return meta.AsString
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"PartitionKey:%s,TimeCreated:%v,TimePulled:%v,TimeTransformed:%v,Metadata:%s,Data:%s",
		m.PartitionKey,
		m.TimeCreated,
		m.TimePulled,
		m.TimeTransformed,
		m.Metadata.GetString(),
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

	for i := 0; i < len(messages); i++ {
		msg := messages[i]
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
	for i := 0; i < len(messages); i++ {
		msg := messages[i]
		msgByteLen := len(msg.Data)

		if msgByteLen > maxMessageByteSize {
			oversized = append(oversized, msg)
		} else {
			safe = append(safe, msg)
		}
	}
	return safe, oversized
}
