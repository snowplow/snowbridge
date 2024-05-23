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
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/itchyny/gojq"
	"github.com/snowplow/snowbridge/pkg/models"
)

/*
// An example of a walk w/reflect.
func walk(v reflect.Value) {
    fmt.Printf("Visiting %v\n", v)
    // Indirect through pointers and interfaces
    for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
        v = v.Elem()
    }
    switch v.Kind() {
    case reflect.Array, reflect.Slice:
        for i := 0; i < v.Len(); i++ {
            walk(v.Index(i))
        }
    case reflect.Map:
        for _, k := range v.MapKeys() {
            walk(v.MapIndex(k))
        }
    default:
        // handle other types
    }
}
*/

// Some magic may be required in the config parsing bit to enable this!
// If it's impractical we can structure the config in an easier to handle way.
var exampleParsedConfig = map[string]any{
	"field1":                   ".app_id",
	"field2":                   map[string]any{"nestedField1": ".contexts_com_acme_just_ints_1[0]"},
	"fieldWithCoalesceExample": map[string]any{"coalesce": []string{"app_id", ".contexts_com_acme_just_ints_1[0]"}},
	// Seeing the implementation, the below is way cleaner!
	"fieldWithOtherCoalesceExample": ".app_id // .contexts_com_acme_just_ints_1[0]",

	"manualUnnest": map[string]any{"just_ints_integerField": ".contexts_com_acme_just_ints_1[0].integerField"},
	// not sure if this should be allowable in config
	"arraySpecified": []string{".app_id", ".event_id"},
}

//			//			// TODO: function to get values
// TODO: function to create objects/iterate config and create objects
// TODO: function to delete key

// In the actual implementation, we would prbably want to iterate the config to compile or parse queries, then later produce the data
// For this implementation sketch, I'll just do all the work here.
func grabLotsOfValues(inputData map[string]any, config map[string]any) map[string]any {
	out := map[string]any{}

	for key, val := range config {
		switch val.(type) {
		// TODO: figure out what kinds of types our config parsing will actually produce, and if this approach or another is needed to handle them
		case map[string]any:

			mapRes := grabLotsOfValues(inputData, val.(map[string]any))
			// TODO: either have this function return nil or check for empty map here.
			out[key] = mapRes
			// TODO: deal with this case
		case []map[string]any:
			// Seems doable but not implemented yet.
		case []string:
			// The way I've structured this function, it's far more complex to support a coalesce option.
			// We could refactor things so that this could be handled slightly more elegantly,
			// but I think it has become fairly clear that the best option is Nick's suggestion - just let jq syntax support this.
			if key == "coalesce" {
				fmt.Println("GOEOEOEOEODMDOEOAO")
				// foundAVal := false
				for index, item := range val.([]string) { // only slice of string allowed
					query, err := gojq.Parse(item)
					if err != nil {
						panic(err)
					}
					outVal, err := grabValue(inputData, query)
					if outVal != nil {
						fmt.Println("============")
						fmt.Println(index)
						fmt.Println(outVal)

						out[key] = outVal
						break
					}
				}

				fmt.Println("--- coalesce found")
				break
			} else {
				outSlice := []any{}
				// Probably could be done with less repeated code
				for _, item := range val.([]string) {
					query, err := gojq.Parse(item)
					if err != nil {
						panic(err)
					}
					outVal, err := grabValue(inputData, query)
					if outVal != nil {
						// Don't add nil keys
						outSlice = append(outSlice, outVal)
					}
				}
				// TODO: Do something to not add empty arrays
				out[key] = outSlice
			}

		case []any:
			// Is there any other allowable slice?
			// COuld we generically deal with all slices?
			fmt.Println("---" + key + " is a []any")
		case string:
			query, err := gojq.Parse(val.(string))
			if err != nil {
				panic(err)
			}
			outVal, err := grabValue(inputData, query)
			if outVal != nil {
				// Don't add nil keys
				out[key] = outVal
			}

		default:
			fmt.Println("something went wrong here")
			fmt.Println(key)
			fmt.Println(val)

		}
	}
	return out
}

// We may want to run gojq.Compile() on startup for each option, pass a *gojq.Code here
func grabValue(inputData map[string]any, query *gojq.Query) (any, error) {

	var grabbedValue any

	iter := query.Run(inputData) // or query.RunWithContext

	v, ok := iter.Next()
	if !ok {
		return nil, errors.New("TODO: ADD ERROR HERE")
	}
	if err, ok := v.(error); ok {

		return nil, err
	}
	grabbedValue = v

	return grabbedValue, nil
}

// Mapper is // TODO: Add description
func Mapper(message *models.Message, intermediateState interface{}) {

	var input map[string]any

	json.Unmarshal(message.Data, &input)

	// query, err := gojq.Parse(".bar.emptyKey // .bar.baz")
	query, err := gojq.Parse(".contexts_com_acme_just_ints_1[0]")
	if err != nil {
		log.Fatalln(err)
	}
	// input := map[string]any{"foo": []any{1, 2, 3}, "bar": map[string]any{"baz": "someValue", "emptyKey": nil}}
	iter := query.Run(input) // or query.RunWithContext
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			if err, ok := err.(*gojq.HaltError); ok && err.Value() == nil {
				break
			}
			log.Fatalln(err)
		}
		fmt.Printf("%#v\n", v)
	}
}

/*




→  jq -n '{userId: 1, userName: "Alice", orgId: 2} | delpaths([paths | select(.[-1] | contains("Id"))])'
{
  "userName": "Alice"
}

---



→  jq -n '{a: {nested: [{key: 1, other: 2}, {key: 2, other: 3}]}} | del(.a.nested[] | select(.other>2) | .key)'
{
  "a": {
    "nested": [
      {
        "key": 1,
        "other": 2
      },
      {
        "other": 3
      }
    ]
  }
}

---

→  jq -n '{eventName: "myEvent", context: {thiskey: "a_value", thatkey: "another_value"}} | .eventName as $name | .context |= with_entries(.key = "\($name)_\(.key)")'
{
  "eventName": "myEvent",
  "context": {
    "myEvent_thiskey": "a_value",
    "myEvent_thatkey": "another_value"
  }
}

---

→  jq -n '{eventName: "myEvent", context: {thiskey: "a_value", thatkey: "another_value"}} | .eventName as $name | .context | with_entries(.key = "\($name)_\(.key)")'
{
  "myEvent_thiskey": "a_value",
  "myEvent_thatkey": "another_value"
}

---

→  jq -n '{context: [{a: 1}]} | .context |= .[0]'
{
  "context": {
    "a": 1
  }
}

----


→  jq -n '{a: 5, b: 1}, {a: 6, b: 2} | if .a > 5 then del(.a) else . end'
{
  "a": 5,
  "b": 1
}
{
  "b": 2
}
*/
