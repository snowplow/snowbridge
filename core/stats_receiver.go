// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

// StatsReceiver describes the interface for how to push observed statistics
// to a downstream store
type StatsReceiver interface {
	Send(buffer *ObserverBuffer)
}
