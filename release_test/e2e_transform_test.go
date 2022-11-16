// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2ETransformations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	t.Run("tsv", testE2ETransformTSVCases)
	t.Run("json", testE2ETransformJSONCases)
	t.Run("pk", testE2ETransformPKCases)
}

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

	sort.Strings(foundData)
	sort.Strings(expectedData)

	assert.Equal(expectedData, foundData)
}

func getFileMountArg(testCase string) string {

	JSScriptFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "script.js"))
	if err != nil {
		panic(err)
	}

	LuaScriptFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "script.lua"))
	if err != nil {
		panic(err)
	}

	// Check if we have a script & mount it if so
	if _, err := os.Stat(JSScriptFilePath); err == nil {
		return fmt.Sprintf("--mount type=bind,source=%s,target=/script.js", JSScriptFilePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	if _, err := os.Stat(LuaScriptFilePath); err == nil {
		return fmt.Sprintf("--mount type=bind,source=%s,target=/script.lua", LuaScriptFilePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	return ""
}

func testE2ETransformTSVCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstruct", "jsPlainFilter", "jsPlainTransform", "luaPlainFilter", "luaPlainTransform"}

	for _, testCase := range casesToTest {

		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		fileMountArg := getFileMountArg(testCase)

		for _, binary := range []string{"-aws-only", ""} {
			stdOut, cmdErr := runDockerCommand(3*time.Second, testCase, configFilePath, binary, fileMountArg)
			if cmdErr != nil {
				assert.Fail(cmdErr.Error())
			}
			expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

			data := getDataFromStdoutResult(stdOut)
			evaluateTestCaseString(t, data, expectedFilePath, testCase+binary)
		}
	}
}

func testE2ETransformJSONCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedToJson", "jsSnowplowFilter", "jsSnowplowTransform"}

	for _, testCase := range casesToTest {

		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		fileMountArg := getFileMountArg(testCase)

		for _, binary := range []string{"-aws-only", ""} {
			stdOut, cmdErr := runDockerCommand(3*time.Second, testCase, configFilePath, binary, fileMountArg)
			if cmdErr != nil {
				assert.Fail(cmdErr.Error())
			}

			expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

			evaluateTestCaseJSON(t, stdOut, expectedFilePath, testCase+binary)
		}
	}

}

func testE2ETransformPKCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedSetPk", "jsSnowplowSetPk", "jsPlainSetPk", "luaPlainSetPk"}

	for _, testCase := range casesToTest {

		// docker --mount command expects absolute filepath
		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		fileMountArg := getFileMountArg(testCase)

		for _, binary := range []string{"-aws-only", ""} {
			stdOut, cmdErr := runDockerCommand(3*time.Second, testCase, configFilePath, binary, fileMountArg)
			if cmdErr != nil {
				assert.Fail(cmdErr.Error())
			}

			expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

			evaluateTestCasePK(t, stdOut, expectedFilePath, testCase+binary)
		}
	}
}
