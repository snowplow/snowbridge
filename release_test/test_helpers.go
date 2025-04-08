package releasetest

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// InputFilePath is the path to the input test file
var InputFilePath, _ = filepath.Abs("input.txt")

// Template used for all docker run commands
var releaseTestCmdTemplate = `cat %s | docker run -i \
--name %s \
--network=integration_default \
--add-host host.docker.internal:host-gateway \
--mount type=bind,source=%s,target=/config.hcl \
--env SNOWBRIDGE_CONFIG_FILE=/config.hcl %s \
--env ACCEPT_LIMITED_USE_LICENSE=true \
snowplow/snowbridge:%s`

// RunDockerCommand executes a docker command for testing
func RunDockerCommand(secondsBeforeShutdown time.Duration, testName string, configFilePath string, binaryVersion string, additionalOpts string) ([]byte, error) {
	containerName := testName + "-" + binaryVersion
	cmdFull := fmt.Sprintf(releaseTestCmdTemplate, InputFilePath, containerName, configFilePath, additionalOpts, binaryVersion)

	cmd := exec.Command("bash", "-c", cmdFull)

	// Capture both stdout and stderr
	output, err := cmd.CombinedOutput()

	// Clean up the container
	defer func() {
		rmCmd := exec.Command("bash", "-c", "docker rm "+containerName)
		rmCmd.Run() // Ignore errors during cleanup
	}()

	if err != nil {
		return nil, fmt.Errorf("docker command failed: %v\nOutput: %s", err, string(output))
	}

	return output, nil
}

// getSliceFromTestInput reads a file and returns its contents as a slice of strings
func getSliceFromTestInput(filepath string) []string {
	inputData, err := os.ReadFile(filepath)
	if err != nil {
		panic(err)
	}

	return strings.Split(string(inputData), "\n")
}

// EvaluateTestCaseString evaluates string test cases
func EvaluateTestCaseString(t *testing.T, foundData []string, expectedFilePath string, testCase string) {
	expectedData := getSliceFromTestInput(expectedFilePath)

	// Sort both slices to ensure consistent comparison
	sort.Strings(foundData)
	sort.Strings(expectedData)

	assert.Equal(t, expectedData, foundData, testCase)
}

// EvaluateTestCaseJSONString evaluates JSON string test cases
func EvaluateTestCaseJSONString(t *testing.T, foundData []string, expectedFilePath string, testCase string) {
	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	expectedData := strings.Split(string(expectedChunk), "\n")

	require.Equal(t, len(expectedData), len(foundData), testCase)

	// Make maps of eid:data, so that we can match like for like events later
	foundWithEids := make(map[string]string)
	expectedWithEids := make(map[string]string)

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

	// Compare the found and expected data
	for eid, foundValue := range foundWithEids {
		expectedValue, exists := expectedWithEids[eid]
		require.True(t, exists, "Event ID %s not found in expected data", eid)
		assert.JSONEq(t, expectedValue, foundValue, testCase)
	}
}
