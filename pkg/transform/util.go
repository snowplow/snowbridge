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
	"crypto/md5"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"
)

const (
	hashByteSize     = 24
	pbkdf2Iterations = 1000
)

var (
	supportedHashFunctions = map[string]func() hash.Hash{
		"sha1":   sha1.New,
		"sha256": sha256.New,
		"md5":    md5.New,
	}
)

// RemoveNullFields removes null fields, empty maps and empty slices from the input object, as long as map keys are strings.
// It also removes null and empty elements from slices.
func RemoveNullFields(data any) {
	switch input := data.(type) {
	case map[string]any:
		removeNullFromMap(input)
	case []any:
		// For slices not in maps, we need to process them and potentially replace them
		// This is handled by the caller, so we just process in-place
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

		// Handle slices stored in maps specially
		if asSlice, ok := field.([]any); ok {
			// Process the slice to remove nil/empty elements
			filteredSlice := removeNullFromSliceAndReturn(asSlice)
			if len(filteredSlice) == 0 {
				delete(input, key)
			} else {
				input[key] = filteredSlice
			}
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
	}
}

func removeNullFromSlice(input []any) {
	// First recurse into all elements to process nested structures
	for i := range input {
		if asSlice, ok := input[i].([]any); ok {
			cleaned := removeNullFromSliceAndReturn(asSlice)
			input[i] = cleaned
		} else {
			RemoveNullFields(input[i])
		}
	}

	// Build a new slice with only non-empty elements, using the cleaned value
	filtered := input[:0]
	for i := range input {
		if !shouldRemoveElement(input[i]) {
			filtered = append(filtered, input[i])
		}
	}

	// Resize the original slice to match the filtered result
	for i := range filtered {
		input[i] = filtered[i]
	}
	if len(filtered) < len(input) {
		for i := len(filtered); i < len(input); i++ {
			input[i] = nil
		}
	}
	input = input[:len(filtered)]
}

func removeNullFromSliceAndReturn(input []any) []any {
	// First recurse into all elements to process nested structures
	for i := range input {
		if asSlice, ok := input[i].([]any); ok {
			cleaned := removeNullFromSliceAndReturn(asSlice)
			input[i] = cleaned
		} else {
			RemoveNullFields(input[i])
		}
	}

	// Build a new slice with only non-empty elements, using the cleaned value
	filtered := make([]any, 0, len(input))
	for i := range input {
		if !shouldRemoveElement(input[i]) {
			filtered = append(filtered, input[i])
		}
	}
	return filtered
}

// shouldRemoveElement checks if an element should be removed from a slice
func shouldRemoveElement(item any) bool {
	if item == nil {
		return true
	}

	// Check for empty maps
	if asMap, ok := item.(map[string]any); ok && len(asMap) == 0 {
		return true
	}

	// Check for empty slices - process them first to see if they become empty
	if asSlice, ok := item.([]any); ok {
		// Process the slice to remove nil/empty elements
		RemoveNullFields(asSlice)
		return len(asSlice) == 0
	}

	return false
}

// DoHashing applies selected hash function (with or without salt provided) on the input string of data
// and returns hashed string or an error if operation failed
func DoHashing(input, hashFunctionName, hashSalt string) (string, error) {
	salt := []byte(hashSalt)

	hashFunction, ok := supportedHashFunctions[hashFunctionName]
	if !ok {
		return "", fmt.Errorf("unsupported hash function: [%s]", hashFunctionName)
	}

	hbts, err := pbkdf2.Key(hashFunction, input, salt, pbkdf2Iterations, hashByteSize)
	if err != nil {
		return "", fmt.Errorf("failed to hash the data: %w", err)
	}

	return fmt.Sprintf("%x", hbts), nil
}
