package generic

import (
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
)

// Implemented by each component, like kinesis target
type configurableComponent[CONFIG any, COMPONENT any] interface {
	DefaultConfig() *CONFIG
	FromConfig(config *CONFIG) (*COMPONENT, error)
}

// Also implemented by each component, but generic type parameter 'OUT' specifies interface requested by consumer, e.g. 'Target' interface.
type providerOf[OUT any] interface {
	Provide(input hcl.Body, ctx *hcl.EvalContext) (OUT, error)
}

func createComponent[CONFIG any, COMPONENT any](component configurableComponent[CONFIG, COMPONENT], input hcl.Body, ctx *hcl.EvalContext) (*COMPONENT, error) {
	config := component.DefaultConfig()
	decode[CONFIG](input, ctx, config)
	readyComponent, err := component.FromConfig(config)

	if err != nil {
		return nil, err
	}
	return readyComponent, nil
}

func decode[CONFIG any](input hcl.Body, evalContext *hcl.EvalContext, configuration *CONFIG) error {
	if input == nil {
		return nil
	}

	if configuration == nil {
		return nil
	}

	diag := gohcl.DecodeBody(input, evalContext, configuration)
	if len(diag) > 0 {
		return diag
	}

	return nil
}
