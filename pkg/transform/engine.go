// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

// FunctionMaker is the interface that wraps the MakeFunction method
type FunctionMaker interface {
	// MakeFunction returns a TransformationFunction that runs
	// a given function in a runtime engine.
	MakeFunction(funcName string) TransformationFunction
}

// SmokeTester is the interface that wraps the SmokeTest method.
type SmokeTester interface {
	// SmokeTest runs a test spin of the engine trying to get as close to
	// running the given function as possible.
	SmokeTest(funcName string) error
}

// Engine is the interface that groups
// FunctionMaker and SmokeTester.
type Engine interface {
	FunctionMaker
	SmokeTester
}

// EngineProtocol is the I/O type of an Engine.
type EngineProtocol struct {
	FilterOut    bool
	PartitionKey string
	Data         interface{}
}
