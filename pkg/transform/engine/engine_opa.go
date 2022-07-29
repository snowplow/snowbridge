package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/open-policy-agent/opa/sdk"
	sdktest "github.com/open-policy-agent/opa/sdk/test"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

// OPAEngineConfig configures the OPA Engine.
type OPAEngineConfig struct {
	PolicyPath string `hcl:"policy_path"`
}

// OPAEngine handles the provision of a OPA runtime to run transformations.
type OPAEngine struct {
	Server *sdktest.Server
	OPA    *sdk.OPA
}

// NewOPAEngine returns a OPA Engine from a OPAEngineConfig.
func NewOPAEngine(c *OPAEngineConfig) (*OPAEngine, error) {

	ctx := context.Background()

	// create a mock HTTP bundle server
	server, err := sdktest.NewServer(sdktest.MockBundle("/bundles/bundle.tar.gz", map[string]string{
		"example.rego": `
				package authz

				default allow := false

				allow {
					input.app_id == "sesame"
				}
			`,
	}))
	if err != nil {

		panic(err)
	}

	// defer server.Stop()

	// provide the OPA configuration which specifies
	// fetching policy bundles from the mock server
	// and logging decisions locally to the console
	config := []byte(fmt.Sprintf(`{
		"services": {
			"test": {
				"url": %q
			}
		},
		"bundles": {
			"test": {
				"resource": "/bundles/bundle.tar.gz"
			}
		},
		"decision_logs": {
			"console": true
		}
	}`, server.URL()))

	// create an instance of the OPA object
	opa, err := sdk.New(ctx, sdk.Options{
		Config: bytes.NewReader(config),
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

		result, err := e.OPA.Decision(context.TODO(), sdk.DecisionOptions{Path: "/authz/allow", Input: input})
		if err != nil {
			// panic(err)
			fmt.Println(err)
		}
		decision, ok := result.Result.(bool)
		if !ok {
			fmt.Println("Not a boolean: ", result.Result)
		}
		if !decision {
			// we might want to reverse the logic here - it's either true to keep message or true to filter/discard message
			return nil, message, nil, nil
		}

		return message, nil, nil, nil
	}
}
