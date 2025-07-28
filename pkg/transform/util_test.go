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

package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveNullFields_MapInput(t *testing.T) {
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
			nil,
			map[string]any{},
			[]any{},
			map[string]any{
				"onlyNil": nil,
			},
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

	output := RemoveNullFields(data)

	assert.Equal(expected, output)

}

func TestRemoveNullFields_SliceInput(t *testing.T) {
	assert := assert.New(t)

	data := []any{
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
		// nil and empty slice elements
		nil,
		map[string]any{},
		[]any{},
		map[string]any{
			"onlyNil": nil,
		},
	}
	expected := []any{
		"stringvalue",
		map[string]any{
			"nonEmptyField": "stringvalue",
		},
		[]any{
			map[string]any{
				"nonEmptyField": "stringvalue",
			},
		},
		// nil, empty map, empty slice, and map with only nil should be removed
	}
	output := RemoveNullFields(data)

	assert.Equal(expected, output)
}
