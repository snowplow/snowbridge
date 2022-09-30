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
	"time"

	"github.com/pkg/errors"
	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var inputFilePath, inputErr = filepath.Abs("input.txt")

// TODO: Extend the input data
// Add more sample events
// Update transformation expected_data files.

// TODO: Remove unused expected_data.txt files.

// Template used for all docker run commands
// We pipe in input.txt regardless of whether it's used - if a source is configured it's just ignored.
var cmdTemplate = `cat %s | docker run -i \
--name %s \
--network=integration_default \
--add-host host.docker.internal:host-gateway \
--mount type=bind,source=%s,target=/config.hcl \
--env STREAM_REPLICATOR_CONFIG_FILE=/config.hcl %s \
snowplow/stream-replicator-%s:` + cmd.AppVersion

// explanation of arguments:
// -i keeps stdin open
// --mount mounts the config file
// --env sets env var for config file resolution
// --add-host host.docker.internal:host-gateway adds a host mapping to the container,
// which infers and automatically maps the container's host.docker.internal IP to the host machine's localhost
// This allows us to refer to host.docker.internal to reference the host machine's localhost in GH actions as well as on local machines

// Helper function to run docker command
// This assumes that docker assets are built (make all) and integration resources exist (make integration-up)
func runDockerCommand(secondsBeforeShutdown time.Duration, testName string, configFilePath string, binaryVersion string, additionalOpts string) ([]byte, error) {
	if inputErr != nil {
		errors.Wrap(inputErr, "Error getting input file: ")
		panic(inputErr)
	}

	containerName := testName + "-" + binaryVersion
	cmdFull := fmt.Sprintf(cmdTemplate, inputFilePath, containerName, configFilePath, additionalOpts, binaryVersion)

	cmd := exec.Command("bash", "-c", cmdFull)

	// Ensure we print stderr to logs, to make debugging a bit more manageable
	cmd.Stderr = os.Stderr

	// Goroutine to stop SR after a bit - we do this because:
	// a) source tests don't self-stop
	// b) if some other test hangs for whatever reason, we should exit and fail (exiting isn't a feature we need to test for)
	// Note that for the test which use stdin source, we expect to exit with a stopped container before we get to this function - and so it won't be called.
	go func() {
		time.Sleep(secondsBeforeShutdown)
		cmd := exec.Command("bash", "-c", "docker stop "+containerName)

		// Ensure we print stderr to logs, to make debugging a bit more manageable
		cmd.Stderr = os.Stderr
		cmd.Output()
	}()

	out, err := cmd.Output()

	defer func() {
		// Remove container before exiting, existing stopped container will cause next docker run to fail.
		rmCmd := exec.Command("bash", "-c", "docker rm "+containerName)

		// Ensure we print stderr to logs, to make debugging a bit more manageable
		rmCmd.Stderr = os.Stderr
		rmCmd.Output()

	}()

	err = errors.Wrap(err, containerName+": Error running Docker Command: "+cmdFull)

	return out, err
}

// Helper function to grab just the 'Data' portion from the result
func getDataFromStdoutResult(result []byte) []string {
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

// Helper function to evaluate tests for String data (TSV and others)
func evaluateTestCaseString(t *testing.T, foundData []string, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	expectedData := strings.Split(string(expectedChunk), "\n")

	// Check that we got the correct number of results
	require.Equal(t, len(expectedData), len(foundData), testCase)

	// Check that the data is equal
	sort.Strings(expectedData)
	sort.Strings(foundData)

	for i, expected := range expectedData {
		assert.Equal(expected, foundData[i], testCase)
	}
}
