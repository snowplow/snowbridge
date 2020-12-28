// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"time"
)

// Event holds the structure of a generic event to be sent to a target
type Event struct {
	PartitionKey string
	Data         []byte

	// TimeCreated is when the event was created originally
	TimeCreated time.Time

	// TimePulled is when the event was pulled from the source
	TimePulled time.Time

	// AckFunc must be called on a successful event emission to ensure
	// any cleanup process for the source is actioned
	AckFunc func()
}
