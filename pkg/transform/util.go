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
