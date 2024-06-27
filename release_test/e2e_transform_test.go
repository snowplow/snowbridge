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

package releasetest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	foundData := getDataFromStdoutResult(actual)

	evaluateTestCaseJSONString(t, foundData, expectedFilePath, testCase)

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

	assert.Equal(expectedData, foundData, testCase)
}

func getFileMountArg(testCase string) string {

	JSScriptFilePath, err := filepath.Abs(filepath.Join("cases", "transformations", testCase, "script.js"))
	if err != nil {
		panic(err)
	}

	// Check if we have a script & mount it if so
	if _, err := os.Stat(JSScriptFilePath); err == nil {
		return fmt.Sprintf("--mount type=bind,source=%s,target=/script.js", JSScriptFilePath)
	} else if !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}

	return ""
}

func testE2ETransformTSVCases(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstruct", "jsPlainFilter", "jsPlainTransform"}

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

	casesToTest := []string{"spEnrichedSetPk", "jsSnowplowSetPk", "jsPlainSetPk"}

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
