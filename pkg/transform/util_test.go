package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveNullFields(t *testing.T) {
	assert := assert.New(t)

	data := map[string]any{
		// simple map with some nils and some others
		"map": map[string]any{
			"emptyField":    nil,
			"nonEmptyField": "stringvalue",
			"innerMap": map[string]any{
				"emptyInnerField": nil,
				"emptyInnerMap":   map[string]any{},
				"nonEmptyField":   "stringvalue",
			},
			// inner slice
			"innerSliceofMap": []any{
				map[string]any{
					"nonEmptyField": "stringvalue",
					"emptyField":    nil,
				},
			},
			// inner empty map
			"innerEmptyMap": map[string]any{},
			//inner empty array
			"innerEmptySlice": []any{},
		},
		"slice": []any{
			"stringvalue",
			map[string]any{
				"emptyInnerField": nil,
				"emptyInnerMap":   map[string]any{},
				"nonEmptyField":   "stringvalue",
			},
			[]any{
				map[string]any{
					"nonEmptyField": "stringvalue",
					"emptyField":    nil,
				},
			},

			// For maps, we use the delete() builtin.
			// For slices, to remove nil or empty _elements_ requires a wider change, which is outside the scope of current requirements.
			// should we encounter this requirement, I think the simplest approaches are to either have RemoveNulls return an output, or
			// have it take a pointer to a typecast variable as an input. (perhaps splitting it into two functions in the process)
			// the former seems more sensible

			// nil,
			// map[string]any{},
			// []any{},
			// map[string]any{
			// 	"onlyNil": nil,
			// },
		},
		// empty map and empty slice - these should be removed too
		"emptyMap":   map[string]any{},
		"emptySlice": []any{},
		"onlyNil": map[string]any{
			"empty": nil,
		},
	}

	expected := map[string]any{
		"map": map[string]any{
			"nonEmptyField": "stringvalue",
			"innerMap": map[string]any{
				"nonEmptyField": "stringvalue",
			},
			// inner slice
			"innerSliceofMap": []any{
				map[string]any{
					"nonEmptyField": "stringvalue",
				},
			},
		},
		"slice": []any{
			"stringvalue",
			map[string]any{
				"nonEmptyField": "stringvalue",
			},
			[]any{
				map[string]any{
					"nonEmptyField": "stringvalue",
				},
			},
		},
	}

	RemoveNullFields(data)

	assert.Equal(expected, data)

}
