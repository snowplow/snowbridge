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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/cmd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var inputFilePath, inputErr = filepath.Abs("input.txt")

// Template used for all docker run commands
// We pipe in input.txt regardless of whether it's used - if a source is configured it's just ignored.
var cmdTemplate = `cat %s | docker run -i \
--name %s \
--network=integration_default \
--add-host host.docker.internal:host-gateway \
--mount type=bind,source=%s,target=/config.hcl \
--env SNOWBRIDGE_CONFIG_FILE=/config.hcl %s \
--env ACCEPT_LIMITED_USE_LICENSE=true \
snowplow/snowbridge:%s%s`

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
	// Check for inputErr as it won't throw outside the function
	if inputErr != nil {
		_ = errors.Wrap(inputErr, "Error getting input file: ")
		panic(inputErr)
	}

	containerName := testName + "-" + binaryVersion
	cmdFull := fmt.Sprintf(cmdTemplate, inputFilePath, containerName, configFilePath, additionalOpts, cmd.AppVersion, binaryVersion)

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
		if _, err := cmd.Output(); err != nil {
			logrus.Error(err.Error())
		}
	}()

	out, err := cmd.Output()

	defer func() {
		// Remove container before exiting, existing stopped container will cause next docker run to fail.
		rmCmd := exec.Command("bash", "-c", "docker rm "+containerName)

		// Ensure we print stderr to logs, to make debugging a bit more manageable
		rmCmd.Stderr = os.Stderr
		if _, err := rmCmd.Output(); err != nil {
			logrus.Error(err.Error())
		}
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

func evaluateTestCaseJSONString(t *testing.T, foundData []string, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	expectedData := strings.Split(string(expectedChunk), "\n")
	require.Equal(t, len(expectedData), len(foundData), testCase)

	// Make maps of eid:data, so that we can match like for like events later
	foundWithEids := make(map[string]string)
	expectedWithEids := make(map[string]string)

	for _, row := range foundData {
		var asMap map[string]any
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

		var asMap map[string]any
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
