// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper function to evaluate tests for JSON data
func evaluateTestCaseJSON(t *testing.T, actual []byte, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	foundData := getDataFromResult(actual)

	expectedData := strings.Split(string(expectedChunk), "\n")

	// We sort by length as a sort of janky workaround, since for JSON we don't have a guarntee of order
	sort.Slice(expectedData, func(i, j int) bool {
		return len(expectedData[i]) < len(expectedData[j])
	})

	sort.Slice(foundData, func(i, j int) bool {
		return len(foundData[i]) < len(foundData[j])
	})

	// Check that we got the correct number of results
	assert.Equal(len(expectedData), len(foundData), testCase)

	for i, expected := range expectedData {
		// Check that the data is equal
		assert.JSONEq(expected, foundData[i], testCase)
	}
}

// Helper function to evaluate tests for Partition Keys
func evaluateTestCasePK(t *testing.T, actual []byte, testCase string) {
	assert := assert.New(t)

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

	assert.Equal(2, len(foundData))

	// This is simple enough to just hardcode the expected data.
	assert.Equal([]string{"test-data1", "test-data2"}, foundData)
}

func TestE2ETransformTSVCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstruct", "jsPlainFilter", "jsPlainTransform", "luaPlainFilter", "luaPlainTransform"}

	for _, testCase := range casesToTest {

		// docker --mount command expects absolute filepath
		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		stdOut, cmdErr := runDockerCommand(cmdTemplate, testCase, configFilePath, "")

		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for "+testCase)
		}

		expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

		evaluateTestCaseString(t, stdOut, expectedFilePath, testCase)
	}
}

func TestE2ETransformJSONCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedToJson", "jsSnowplowFilter", "jsSnowplowTransform", "luaSnowplowFilter"}

	// TODO: skipping "luaSnowplowTransform" for now due to: https://github.com/snowplow-devops/stream-replicator/issues/214
	// When that's fixed, add it back in here.

	for _, testCase := range casesToTest {

		// docker --mount command expects absolute filepath
		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		stdOut, cmdErr := runDockerCommand(cmdTemplate, testCase, configFilePath, "")

		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for "+testCase)
		}

		expectedFilePath := filepath.Join("cases", "transformations", testCase, "expected_data.txt")

		evaluateTestCaseJSON(t, stdOut, expectedFilePath, testCase)
	}

}

func TestE2ETransformPKCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedSetPk", "jsSnowplowSetPk", "jsPlainSetPk", "luaPlainSetPk", "luaSnowplowSetPk"}

	for _, testCase := range casesToTest {

		// docker --mount command expects absolute filepath
		configFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		stdOut, cmdErr := runDockerCommand(cmdTemplate, testCase, configFilePath, "")

		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for "+testCase)
		}

		evaluateTestCasePK(t, stdOut, testCase)
	}
}
