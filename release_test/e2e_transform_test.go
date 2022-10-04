// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to evaluate tests for JSON data
func evaluateTestCaseJSON(t *testing.T, actual []byte, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	foundData := getDataFromStdoutResult(actual)

	expectedData := strings.Split(string(expectedChunk), "\n")

	require.Equal(t, len(expectedData), len(foundData), testCase)

	// Make maps of eid:data, so that we can match like for like events later
	foundWithEids := make(map[string]string)
	expectedWithEids := make(map[string]string)

	for _, row := range foundData {
		var asMap map[string]interface{}
		err = json.Unmarshal([]byte(row), &asMap)
		if err != nil {
			panic(err)
		}
		eid, ok := asMap["event_id"].(string)
		require.True(t, ok)
		// Make a map entry with Eid Key
		foundWithEids[eid] = row

	}

	for _, row := range expectedData {

		var asMap map[string]interface{}
		err = json.Unmarshal([]byte(row), &asMap)
		if err != nil {
			panic(err)
		}
		eid, ok := asMap["event_id"].(string)
		require.True(t, ok)
		expectedWithEids[eid] = row
	}

	// Iterate and assert against the values. Since we require equal lengths above, we don't need to check for entries existing in one but not the other.
	for foundEid, foundValue := range foundWithEids {

		assert.JSONEq(foundValue, expectedWithEids[foundEid], testCase)

	}

}

// Helper function to evaluate tests for Partition Keys
func evaluateTestCasePK(t *testing.T, actual []byte, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	expectedData := strings.Split(string(expectedChunk), "\n")

	// Trim trailing newline then split on newline
	foundOutput := strings.Split(strings.TrimSuffix(string(actual), "\n"), "\n")

	// Get just the 'Data' section from output
	var foundData []string

	for _, foundRow := range foundOutput {
		// Janky way to grab the PK values
		pkSplit := strings.Split(foundRow, "PartitionKey:")
		data := strings.Split(pkSplit[1], ",")
		foundData = append(foundData, data[0])
	}

	// We can just define the test here too
	sort.Strings(foundData)
	sort.Strings(expectedData)

	//assert.Equal(2, len(foundData))

	// This is simple enough to just hardcode the expected data.
	assert.Equal(expectedData, foundData)
}

// THis is failing for spEnrichedFilterUnstruct because the event_name fields in the sample are all null...
func TestE2ETransformTSVCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstruct", "jsPlainFilter", "jsPlainTransform", "luaPlainFilter", "luaPlainTransform"}

	for _, testCase := range casesToTest {

		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		for _, binary := range []string{"aws", "gcp"} {
			stdOut, cmdErr := runDockerCommand(3*time.Second, testCase, configFilePath, binary, "")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error())
			}
			expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, expectedFilePath, testCase+binary)
		}
	}
}

func TestE2ETransformJSONCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedToJson", "jsSnowplowFilter", "jsSnowplowTransform"}

	// TODO: skipping "luaSnowplowTransform" and "luaSnowplowFilter" for now due to: https://github.com/snowplow-devops/stream-replicator/issues/214
	// When that's fixed, add it back in here.

	for _, testCase := range casesToTest {

		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		for _, binary := range []string{"aws", "gcp"} {
			stdOut, cmdErr := runDockerCommand(3*time.Second, testCase, configFilePath, binary, "")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error())
			}

			expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

			evaluateTestCaseJSON(t, stdOut, expectedFilePath, testCase+binary)
		}
	}

}

/*
// TODO: Log bug:

interface conversion: interface {} is nil, not string [recovered]
	panic: interface conversion: interface {} is nil, not string
// Most likely occurs because stdin exits, or the container is disturbed before it's done? https://stackoverflow.com/questions/71622424/golang-panic-interface-conversion-interface-is-nil-not-string

// In attempting to repro, we get only:

// fatal error: found pointer to free object
or
// panic: runtime error: invalid memory address or nil pointer dereference

This is super fishy. goccy/go-json seems potentially responsible for at least some of it.
Alternatively it might be to do with hitting the EOF before the program is finished.

// Issue pinned down to go-json package. Swapping it out for encoding/json fully resolves it.
	// encoding/json is slower, but for now I think we sacrifice speed for reliability.
*/

func TestE2ETransformPKCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedSetPk", "jsSnowplowSetPk", "jsPlainSetPk", "luaPlainSetPk", "luaSnowplowSetPk"}

	for _, testCase := range casesToTest {

		// docker --mount command expects absolute filepath
		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		for _, binary := range []string{"aws", "gcp"} {
			stdOut, cmdErr := runDockerCommand(3*time.Second, testCase, configFilePath, binary, "")
			if cmdErr != nil {
				assert.Fail(cmdErr.Error())
			}

			expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

			evaluateTestCasePK(t, stdOut, expectedFilePath, testCase+binary)
		}
	}
}
