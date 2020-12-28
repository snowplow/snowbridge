// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

// SourceFunctions contain the callback functions required by each source
type SourceFunctions struct {
	WriteToTarget func(messages []*Message) error
}

// Source describes the interface for how to read the data pulled from the source
type Source interface {
	Read(sf *SourceFunctions) error
}
