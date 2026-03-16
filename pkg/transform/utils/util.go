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
// It doesn't modify the original object but returns a new one with null fields removed.
func RemoveNullFields(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return RemoveNullFromMap(v)
	case []any:
		return removeNullFromSlice(v)
	default:
		return v
	}
}

func RemoveNullFromMap(input map[string]any) map[string]any {
	result := make(map[string]any)

	for key, value := range input {
		cleaned := RemoveNullFields(value)
		if !isEmpty(cleaned) {
			result[key] = cleaned
		}
	}

	return result
}

func removeNullFromSlice(input []any) []any {
	result := make([]any, 0)

	for _, item := range input {
		cleaned := RemoveNullFields(item)
		if !isEmpty(cleaned) {
			result = append(result, cleaned)
		}
	}

	return result
}

func isEmpty(value any) bool {
	if value == nil {
		return true
	}

	switch v := value.(type) {
	case map[string]any:
		return len(v) == 0
	case []any:
		return len(v) == 0
	default:
		return false
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
