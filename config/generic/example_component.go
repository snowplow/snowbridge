package generic

import (
	"github.com/hashicorp/hcl/v2"
)

type myComponentConfig struct {
	Test string `hcl:"test_string"`
}

type myComponent struct {
	config myComponentConfig
}

// This one is used by component's clients. If you want ot use component, just use this struct
type testProvider struct{}

// Return component as interface, here as 'Target'
func (tp testProvider) Provide(input hcl.Body, ctx *hcl.EvalContext) (target, error) {
	return createComponent[myComponentConfig, myComponent](tp, input, ctx)
}

func (testProvider) DefaultConfig() *myComponentConfig {
	return &myComponentConfig{"some default value"}
}

func (testProvider) FromConfig(config *myComponentConfig) (*myComponent, error) {
	return &myComponent{*config}, nil
}

// Implementing desired interfaces, like 'Target'
func (mc myComponent) Write(input string) string {
	return "Written: " + input + ". This is value in config - " + mc.config.Test
}
