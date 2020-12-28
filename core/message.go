// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"time"
	"fmt"
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
