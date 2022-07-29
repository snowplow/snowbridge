package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/open-policy-agent/opa/sdk"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// OPAEngineConfig configures the OPA Engine.
type OPAEngineConfig struct {
	PolicyPath string `hcl:"policy_path"`
	OPAConfig string `hcl:"opa_config"`
}

// OPAEngine handles the provision of a OPA runtime to run transformations.
type OPAEngine struct {
	OPA    *sdk.OPA
}

// NewOPAEngine returns a OPA Engine from a OPAEngineConfig.
func NewOPAEngine(c *OPAEngineConfig) (*OPAEngine, error) {
	ctx := context.Background()

	// create an instance of the OPA object
	opa, err := sdk.New(ctx, sdk.Options{
		Config: bytes.NewReader([]byte(c.OPAConfig)),
	})
	if err != nil {
		panic(err)
	}

	// defer opa.Stop(ctx)

	return &OPAEngine{OPA: opa}, err
}

// The OPAEngineAdapter type is an adapter for functions to be used as
// pluggable components for OPA Engine. It implements the Pluggable interface.
type OPAEngineAdapter func(i interface{}) (interface{}, error)

// AdaptOPAEngineFunc returns a OPAEngineAdapter.
func AdaptOPAEngineFunc(f func(c *OPAEngineConfig) (*OPAEngine, error)) OPAEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*OPAEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected OPAEngineConfig")
		}

		return f(cfg)
	}
}

// Create implements the ComponentCreator interface.
func (f OPAEngineAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f OPAEngineAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &OPAEngineConfig{}

	return cfg, nil
}

// OPAEngineConfigFunction returns the Pluggable transformation layer implemented in OPA.
func OPAEngineConfigFunction(t *OPAEngineConfig) (*OPAEngine, error) {
	return NewOPAEngine(&OPAEngineConfig{
		PolicyPath: t.PolicyPath,
	})
}

// MakeFunction implements functionMaker.
func (e *OPAEngine) MakeFunction() transform.TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		parsed, err := transform.IntermediateAsSpEnrichedParsed(interState, message)
		if err != nil {
			//just panicing for now cos hackathon
			panic(err)
		}
		input, err := parsed.ToMap()
		if err != nil {
			panic(err)
		}

		result, err := e.OPA.Decision(context.TODO(), sdk.DecisionOptions{Path: "/snp/drop", Input: input})
		if err != nil {
			panic(err)
		}
		drop, ok := result.Result.(bool)
		if !ok {
			fmt.Println("Not a boolean: ", result.Result)
		}

		if drop {
			return nil, message, nil, nil
		}

		return message, nil, nil, nil
	}
}
