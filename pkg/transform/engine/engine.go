//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package engine

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/snowplow/snowbridge/pkg/transform"
)

// make a jsoniter instance that won't escape html
var json = jsoniter.Config{}.Froze()

// functionMaker is the interface that wraps the MakeFunction method
type functionMaker interface {
	// MakeFunction returns a TransformationFunction that runs
	// a given function in a runtime engine.
	MakeFunction(funcName string) transform.TransformationFunction
}

// smokeTester is the interface that wraps the SmokeTest method.
type smokeTester interface {
	// SmokeTest runs a test spin of the engine trying to get as close to
	// running the given function as possible.
	SmokeTest(funcName string) error
}

// Engine is the interface that groups
// functionMaker and smokeTester.
type Engine interface {
	functionMaker
	smokeTester
}

// engineProtocol is the I/O type of Engine.
type engineProtocol struct {
	FilterOut    bool
	PartitionKey string
	Data         interface{}
}
