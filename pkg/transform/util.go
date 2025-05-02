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

// RemoveNullFields removes null fields, empty maps and empty slices from the input object, as long as map keys are strings.
// At present it doesn't remove null or empty elements from slices.
func RemoveNullFields(data any) {
	switch input := data.(type) {
	case map[string]any:
		removeNullFromMap(input)
	case []any:
		removeNullFromSlice(input)
	default:
		return
	}
}

func removeNullFromMap(input map[string]any) {
	for key := range input {
		field := input[key]
		if field == nil {
			delete(input, key)
			continue
		}
		// Recurse first, because the outcome might be an empty field.
		RemoveNullFields(field)

		// Now cast types and check for empties
		asMap, ok := field.(map[string]any)
		if ok && len(asMap) == 0 {
			delete(input, key)
			continue
		}
		asSlice, ok := field.([]any)
		if ok && len(asSlice) == 0 {
			delete(input, key)
			continue
		}

	}
}

func removeNullFromSlice(input []any) {
	for _, item := range input {
		RemoveNullFields(item)
	}
}
