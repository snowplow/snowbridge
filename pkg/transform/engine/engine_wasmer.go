package engine

import (
	"errors"
	"io/ioutil"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/wasmerio/wasmer-go/wasmer"
)

// WASMEngineConfig configures the WASM Engine.
type WASMEngineConfig struct {
	SourceFilepath string `hcl:"source_filepath"`
}

// WASMEngine handles the provision of a WASM runtime to run transformations.
type WASMEngine struct {
	Function func(...interface{}) (interface{}, error)
}

// NewWASMEngine returns a WASM Engine from a WASMEngineConfig.
func NewWASMEngine(c *WASMEngineConfig) (*WASMEngine, error) {
	wasmBytes, _ := ioutil.ReadFile(c.SourceFilepath)

	engine := wasmer.NewEngine()
	store := wasmer.NewStore(engine)

	// Compiles the module
	module, _ := wasmer.NewModule(store, wasmBytes)

	// Instantiates the module
	importObject := wasmer.NewImportObject()
	instance, _ := wasmer.NewInstance(module, importObject)

	// Gets the `sum` exported function from the WebAssembly instance.
	sum, err := instance.Exports.GetFunction("sum")

	return &WASMEngine{sum}, err
}

// The WASMEngineAdapter type is an adapter for functions to be used as
// pluggable components for WASM Engine. It implements the Pluggable interface.
type WASMEngineAdapter func(i interface{}) (interface{}, error)

// AdaptWASMEngineFunc returns a WASMEngineAdapter.
func AdaptWASMEngineFunc(f func(c *WASMEngineConfig) (*WASMEngine, error)) WASMEngineAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*WASMEngineConfig)
		if !ok {
			return nil, errors.New("invalid input, expected WASMEngineConfig")
		}

		return f(cfg)
	}
}

// Create implements the ComponentCreator interface.
func (f WASMEngineAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// ProvideDefault implements the ComponentConfigurable interface.
func (f WASMEngineAdapter) ProvideDefault() (interface{}, error) {
	// Provide defaults for the optional parameters
	// whose default is not their zero value.
	cfg := &WASMEngineConfig{}

	return cfg, nil
}

// WASMEngineConfigFunction returns the Pluggable transformation layer implemented in WASM.
func WASMEngineConfigFunction(t *WASMEngineConfig) (*WASMEngine, error) {
	return NewWASMEngine(&WASMEngineConfig{
		SourceFilepath: t.SourceFilepath,
	})
}

// MakeFunction implements functionMaker.
func (e *WASMEngine) MakeFunction(funcName string) transform.TransformationFunction {

	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {

		// Our dumb example can only use integers to begin with.
		// So let's just use the length of data.
		input := len(message.Data)

		// run the script.
		res, err := e.Function(input)
		if err != nil {
			// just panicing for now as it'll be easier for us to work with in a hackathon
			panic(err)
		}

		// to begin with, let's just say we return 1 to keep the event and 0 to filter it.
		if res == 0 {
			return nil, nil, message, nil
		}

		// If we figure out passing data to the function, then we can do more here.

		// otherwise, we keep the message
		return message, nil, nil, nil

	}
}
