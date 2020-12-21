// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

// Event holds the structure of a generic event to be sent to a target
type Event struct {
	PartitionKey string
	Data         []byte

	// Must be called on a successful event emission
	AckFunc func()
}
