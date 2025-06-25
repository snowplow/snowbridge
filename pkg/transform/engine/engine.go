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
	Data         any
	HTTPHeaders  map[string]string
}
