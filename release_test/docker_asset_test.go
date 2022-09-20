// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package releasetest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/stretchr/testify/assert"
)

// TODO:

// Does it need two separate command templates for sources?

// Command to run docker asset
// Presumes that local docker asset has been built - run make container to do so.

// explanation of arguments:
// -i keeps stdin open
// --mount mounts the config file
// --env sets env var for config file resolution
var cmdTemplate = `cat %s | docker run -i \
--mount type=bind,source=%s,target=/config.hcl \
--env STREAM_REPLICATOR_CONFIG_FILE=/config.hcl \
snowplow/stream-replicator-aws:` + cmd.AppVersion

// Helper function to run docker command
func runDockerCommand(configFilePath string) ([]byte, error) {

	inputFilePath := filepath.Join("input.txt")

	cmdFull := fmt.Sprintf(cmdTemplate, inputFilePath, configFilePath)

	cmd := exec.Command("bash", "-c", cmdFull)

	// Ensure we print stderr to logs, to make debugging a bit more manageable
	cmd.Stderr = os.Stderr

	return cmd.Output()
}

// Helper function to grab just the 'Data' portion from the result
func getDataFromResult(result []byte) []string {
	// Trim trailing newline then split on newline
	foundOutput := strings.Split(strings.TrimSuffix(string(result), "\n"), "\n")

	// Get just the 'Data' section from output
	var foundData []string

	for _, foundRow := range foundOutput {
		// Janky way to grab the 'Data' field
		data := strings.Split(foundRow, ",Data:")
		foundData = append(foundData, data[len(data)-1])
	}
	return foundData
}

// Helper function to evaluate tests for TSV data
func evaluateTestCaseTSV(t *testing.T, actual []byte, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	// TODO: when we move to testing targets, we might be able to factor this better and reuse
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
		assert.Equal(expected, foundData[i], testCase)
	}
}

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
		assert.JSONEq(expected, foundData[i])
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

func TestDockerRunBuiltinFilters(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstruct"}

	for _, testCase := range casesToTest {

		// docker --mount command expects absolute filepath
		configFilePath, err := filepath.Abs(filepath.Join("cases", testCase, "config.hcl"))
		if err != nil {
			panic(err)
		}

		stdOut, cmdErr := runDockerCommand(configFilePath)

		if cmdErr != nil {
			assert.Fail(cmdErr.Error(), "Docker run returned error for "+testCase)
		}

		expectedFilePath := filepath.Join("cases", testCase, "expected_data.txt")

		evaluateTestCaseTSV(t, stdOut, expectedFilePath, testCase)
	}
}

func TestDockerRunSpEnrichedToJson(t *testing.T) {
	assert := assert.New(t)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "spEnrichedToJson", "config.hcl"))
	if err != nil {
		panic(err)
	}

	stdOut, cmdErr := runDockerCommand(configFilePath)

	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for spEnrichedToJson")
	}

	expectedFilePath := filepath.Join("cases", "spEnrichedToJson", "expected_data.txt")

	evaluateTestCaseJSON(t, stdOut, expectedFilePath, "spEnrichedToJson")

}

func TestDockerRunSpEnrichedSetPK(t *testing.T) {
	assert := assert.New(t)

	configFilePath, err := filepath.Abs(filepath.Join("cases", "spEnrichedSetPk", "config.hcl"))
	if err != nil {
		panic(err)
	}

	stdOut, cmdErr := runDockerCommand(configFilePath)

	if cmdErr != nil {
		assert.Fail(cmdErr.Error(), "Docker run returned error for spEnrichedSetPk")
	}

	evaluateTestCasePK(t, stdOut, "spEnrichedSetPk")
}
