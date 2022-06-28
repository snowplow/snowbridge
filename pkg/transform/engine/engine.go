// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package engine

import (
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

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

// getter is the interface that wraps the needed get functions
type getter interface {
	GetName() string
}

// Engine is the interface that groups
// functionMaker and smokeTester.
type Engine interface {
	getter
	functionMaker
	smokeTester
}

// engineProtocol is the I/O type of Engine.
type engineProtocol struct {
	FilterOut    bool
	PartitionKey string
	Data         interface{}
}
