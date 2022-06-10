// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

// componentConfigurable is the interface that wraps the ProvideDefault method.
type componentConfigurable interface {
	// ProvideDefault returns a pointer to a structure that will be
	// written with the decoded configuration.
	ProvideDefault() (interface{}, error)
}

// componentCreator is the interface that wraps the Create method.
type componentCreator interface {
	// Create returns a pointer to an output structure given a pointer
	// to an input structure. This interface is expected to be implemented
	// by components that are creatable through a configuration.
	Create(i interface{}) (interface{}, error)
}

// Pluggable is the interface that groups
// componentConfigurable and componentCreator.
type Pluggable interface {
	componentConfigurable
	componentCreator
}

// decodingHandler is the type of any function that, given a componentConfigurable
// and a decoder, returns a pointer to a structure that was decoded.
type decodingHandler func(c componentConfigurable, d decoder) (interface{}, error)

// withDecoderOptions returns a decodingHandler closed over some DecoderOptions.
func withDecoderOptions(opts *DecoderOptions) decodingHandler {
	return func(c componentConfigurable, d decoder) (interface{}, error) {
		return configure(c, d, opts)
	}
}

// Configure returns the decoded target.
func configure(c componentConfigurable, d decoder, opts *DecoderOptions) (interface{}, error) {
	target, err := c.ProvideDefault() // target is ptr
	if err != nil {
		return nil, err
	}

	if err = d.decode(opts, target); err != nil {
		return nil, err
	}

	return target, nil
}
