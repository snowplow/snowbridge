/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/itchyny/gojq"

	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

// JQMapperConfig represents the configuration for the JQ transformation
type JQMapperConfig struct {
	JQCommand  string `hcl:"jq_command"`
	RunTimeout int    `hcl:"timeout_sec,optional"`
	SpMode     bool   `hcl:"snowplow_mode,optional"`
}

// JQMapper handles jq generic mapping as a transformation
type jqMapper struct {
	JQCode     *gojq.Code
	RunTimeout time.Duration
	SpMode     bool
}

// RunFunction runs a jq mapper transformation
func (jqm *jqMapper) RunFunction() TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		input, err := mkJQInput(jqm, message, interState)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), jqm.RunTimeout)
		defer cancel()

		iter := jqm.JQCode.RunWithContext(ctx, input)
		// no looping since we only keep first value
		v, ok := iter.Next()
		if !ok {
			message.SetError(fmt.Errorf("jq query got no output"))
			return nil, nil, message, nil
		}

		if err, ok := v.(error); ok {
			message.SetError(err)
			return nil, nil, message, nil
		}

		// here v is any, so we Marshal. alternative: gojq.Marshal
		data, err := json.Marshal(v)
		if err != nil {
			message.SetError(fmt.Errorf("error encoding jq query output data"))
			return nil, nil, message, nil
		}

		message.Data = data
		return message, nil, nil, nil
	}
}

// jqMapperAdapter implements the Pluggable interface
type jqMapperAdapter func(i interface{}) (interface{}, error)

// ProvideDefault implements the ComponentConfigurable interface
func (f jqMapperAdapter) ProvideDefault() (interface{}, error) {
	return &JQMapperConfig{
		RunTimeout: 15,
	}, nil
}

// Create implements the ComponentCreator interface
func (f jqMapperAdapter) Create(i interface{}) (interface{}, error) {
	return f(i)
}

// jqMapperAdapterGenerator returns a jqAdapter
func jqMapperAdapterGenerator(f func(c *JQMapperConfig) (TransformationFunction, error)) jqMapperAdapter {
	return func(i interface{}) (interface{}, error) {
		cfg, ok := i.(*JQMapperConfig)
		if !ok {
			return nil, fmt.Errorf("invalid input, expected JQMapperConfig")
		}

		return f(cfg)
	}
}

// JQMapperConfigFunction returns a jq mapper transformation function from a JQMapperConfig
func JQMapperConfigFunction(c *JQMapperConfig) (TransformationFunction, error) {
	query, err := gojq.Parse(c.JQCommand)
	if err != nil {
		return nil, fmt.Errorf("error parsing jq command: %q", err.Error())
	}

	code, err := gojq.Compile(query)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %q", err.Error())
	}

	jq := &jqMapper{
		JQCode:     code,
		RunTimeout: time.Duration(c.RunTimeout) * time.Second,
		SpMode:     c.SpMode,
	}

	return jq.RunFunction(), nil
}

// JQMapperConfigPair is a configuration pair for the jq mapper transformation
var JQMapperConfigPair = config.ConfigurationPair{
	Name:   "jq",
	Handle: jqMapperAdapterGenerator(JQMapperConfigFunction),
}

// mkJQInput ensures the input to JQ query is of expected type
func mkJQInput(jqm *jqMapper, message *models.Message, interState interface{}) (map[string]interface{}, error) {
	if !jqm.SpMode {
		// gojq input can only be map[string]any or []any
		// here we only consider the first, but we could also expand
		var input map[string]interface{}
		err := json.Unmarshal(message.Data, &input)
		if err != nil {
			return nil, err
		}

		return input, nil
	}

	parsedEvent, err := IntermediateAsSpEnrichedParsed(interState, message)
	if err != nil {
		return nil, err
	}

	spInput, err := parsedEvent.ToMap()
	if err != nil {
		return nil, err
	}

	return spInput, nil
}

// Commenting out for reference

// // This works perfectly, is a million times simpler to implement, and prevents us from being blocked in future if we haven't predicted a requirement
// var examplePureJQConfig = `{
// 	field1: .app_id,
// 	field2: { field2: .contexts_com_acme_just_ints_1[0] },
// 	fieldWithOtherCoalesceExample: ( .app_id // .contexts_com_acme_just_ints_1[0] ),
// 	manualUnnest: { just_ints_integerField: .contexts_com_acme_just_ints_1[0].integerField },
// 	arraySpecified: [ .app_id, .event_id ]
//   }`

// func grabFromGenericJQConfig(inputData map[string]any, config string) []byte {
// 	query, err := gojq.Parse(config)
// 	if err != nil {
// 		panic(err)
// 	}

// 	res, err := grabValue(inputData, query)
// 	if err != nil {
// 		panic(err)
// 	}

// 	out, err := json.Marshal(res)
// 	if err != nil {
// 		panic(err)
// 	}

// 	return out
// }

// // Some magic may be required in the config parsing bit to enable this!
// // If it's impractical we can structure the config in an easier to handle way.
// var exampleParsedConfig = map[string]any{
// 	"field1":                   ".app_id",
// 	"field2":                   map[string]any{"nestedField1": ".contexts_com_acme_just_ints_1[0]"},
// 	"fieldWithCoalesceExample": map[string]any{"coalesce": []string{"app_id", ".contexts_com_acme_just_ints_1[0]"}},
// 	// Seeing the implementation, the below is way cleaner!
// 	"fieldWithOtherCoalesceExample": ".app_id // .contexts_com_acme_just_ints_1[0]",

// 	"manualUnnest": map[string]any{"just_ints_integerField": ".contexts_com_acme_just_ints_1[0].integerField"},
// 	// not sure if this should be allowable in config
// 	"arraySpecified": []string{".app_id", ".event_id"},
// }

// //			//			// TODO: function to get values
// //			// 			// TODO: function to create objects/iterate config and create objects
// // TODO: function to delete keys after?

// // For the delete key function, perhaps we factor the below such that traversing can be re-used?

// // In the actual implementation, we would prbably want to iterate the config to compile or parse queries, then later produce the data
// // For this implementation sketch, I'll just do all the work here.
// func grabLotsOfValues(inputData map[string]any, config map[string]any) map[string]any {
// 	out := map[string]any{}

// 	for key, val := range config {
// 		switch val.(type) {
// 		// TODO: figure out what kinds of types our config parsing will actually produce, and if this approach or another is needed to handle them
// 		case map[string]any:

// 			mapRes := grabLotsOfValues(inputData, val.(map[string]any))
// 			// TODO: either have this function return nil or check for empty map here.
// 			out[key] = mapRes
// 		case []map[string]any:
// 			// Seems doable but not implemented yet.
// 		case []string:
// 			// The way I've structured this function, it's a bit more complex to support a coalesce option.
// 			// We could refactor things so that this could be handled slightly more elegantly,
// 			// but I think it has become fairly clear that the best option is Nick's suggestion - just let jq syntax support this.
// 			if key == "coalesce" {
// 				for _, item := range val.([]string) { // only slice of string allowed
// 					query, err := gojq.Parse(item)
// 					if err != nil {
// 						panic(err)
// 					}
// 					outVal, err := grabValue(inputData, query)
// 					if outVal != nil {
// 						out[key] = outVal
// 						break
// 					}
// 				}
// 				break
// 			} else {
// 				outSlice := []any{}
// 				// Probably could be done with less repeated code
// 				for _, item := range val.([]string) {
// 					query, err := gojq.Parse(item)
// 					if err != nil {
// 						panic(err)
// 					}
// 					outVal, err := grabValue(inputData, query)
// 					if outVal != nil {
// 						// Don't add nil keys
// 						outSlice = append(outSlice, outVal)
// 					}
// 				}
// 				// TODO: Do something to not add empty arrays
// 				out[key] = outSlice
// 			}
// 		case string:
// 			query, err := gojq.Parse(val.(string))
// 			if err != nil {
// 				panic(err)
// 			}
// 			outVal, err := grabValue(inputData, query)
// 			if outVal != nil {
// 				// Don't add nil keys
// 				out[key] = outVal
// 			}
// 		default:
// 			fmt.Println("something went wrong here")
// 			fmt.Println(key)
// 			fmt.Println(val)
// 		}
// 	}
// 	return out
// }

// // We may want to run gojq.Compile() on startup for each option, pass a *gojq.Code here
// func grabValue(inputData map[string]any, query *gojq.Query) (any, error) {

// 	var grabbedValue any

// 	iter := query.Run(inputData) // or query.RunWithContext

// 	v, ok := iter.Next()
// 	if !ok {
// 		return nil, errors.New("TODO: ADD ERROR HERE")
// 	}
// 	if err, ok := v.(error); ok {

// 		return nil, err
// 	}
// 	grabbedValue = v

// 	return grabbedValue, nil
// }

// // Mapper is // TODO: Add description
// func Mapper(message *models.Message, intermediateState interface{}) {

// 	var input map[string]any

// 	json.Unmarshal(message.Data, &input)

// 	// query, err := gojq.Parse(".bar.emptyKey // .bar.baz")
// 	query, err := gojq.Parse(".contexts_com_acme_just_ints_1[0]")
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	// input := map[string]any{"foo": []any{1, 2, 3}, "bar": map[string]any{"baz": "someValue", "emptyKey": nil}}
// 	iter := query.Run(input) // or query.RunWithContext
// 	for {
// 		v, ok := iter.Next()
// 		if !ok {
// 			break
// 		}
// 		if err, ok := v.(error); ok {
// 			if err, ok := err.(*gojq.HaltError); ok && err.Value() == nil {
// 				break
// 			}
// 			log.Fatalln(err)
// 		}
// 		fmt.Printf("%#v\n", v)
// 	}
// }
