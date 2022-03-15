// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

// ComponentConfigurable is the interface that wraps the ProvideDefault method.
type ComponentConfigurable interface {
	// ProvideDefault returns a pointer to a structure that will be
	// written with the decoded configuration.
	ProvideDefault() (interface{}, error)
}

// ComponentCreator is the interface that wraps the Create method.
type ComponentCreator interface {
	// Create returns a pointer to an output structure given a pointer
	// to an input structure. This interface is expected to be implemented
	// by components that are creatable through a configuration.
	Create(i interface{}) (interface{}, error)
}

// Pluggable is the interface that groups
// ComponentConfigurable and ComponentCreator.
type Pluggable interface {
	ComponentConfigurable
	ComponentCreator
}

// DecodingHandler is the type of any function that, given a ComponentConfigurable
// and a Decoder, returns a pointer to a structure that was decoded.
type DecodingHandler func(c ComponentConfigurable, d Decoder) (interface{}, error)

// WithDecoderOptions returns a DecodingHandler closed over some DecoderOptions.
func WithDecoderOptions(opts *DecoderOptions) DecodingHandler {
	return func(c ComponentConfigurable, d Decoder) (interface{}, error) {
		return Configure(c, d, opts)
	}
}

// Configure returns the decoded target.
func Configure(c ComponentConfigurable, d Decoder, opts *DecoderOptions) (interface{}, error) {
	target, err := c.ProvideDefault() // target is ptr
	if err != nil {
		return nil, err
	}

	if err = d.Decode(opts, target); err != nil {
		return nil, err
	}

	return target, nil
}
