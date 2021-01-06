// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package models

import (
	"fmt"
	"time"
)

// Message holds the structure of a generic message to be sent to a target
type Message struct {
	PartitionKey string
	Data         []byte

	// TimeCreated is when the message was created originally
	TimeCreated time.Time

	// TimePulled is when the message was pulled from the source
	TimePulled time.Time

	// AckFunc must be called on a successful message emission to ensure
	// any cleanup process for the source is actioned
	AckFunc func()
}

func (m *Message) String() string {
	return fmt.Sprintf(
		"PartitionKey:%s,TimeCreated:%v,TimePulled:%v,Data:%s",
		m.PartitionKey,
		m.TimeCreated,
		m.TimePulled,
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
