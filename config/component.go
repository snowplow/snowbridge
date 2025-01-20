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

// decodingHandler is the type of any function that, given a ComponentConfigurable
// and a Decoder, returns a pointer to a structure that was decoded.
type decodingHandler func(c ComponentConfigurable, d Decoder) (interface{}, error)

// withDecoderOptions returns a decodingHandler closed over some DecoderOptions.
func withDecoderOptions(opts *DecoderOptions) decodingHandler {
	return func(c ComponentConfigurable, d Decoder) (interface{}, error) {
		return configure(c, d, opts)
	}
}

// Configure returns the decoded target.
func configure(c ComponentConfigurable, d Decoder, opts *DecoderOptions) (interface{}, error) {
	target, err := c.ProvideDefault() // target is ptr
	if err != nil {
		return nil, err
	}

	if err = d.Decode(opts, target); err != nil {
		return nil, err
	}

	return target, nil
}
