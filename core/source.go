// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

// Source describes the interface for how to read the data pulled from the source
type Source interface {
	Read() ([]*Event, bool, error)
}
