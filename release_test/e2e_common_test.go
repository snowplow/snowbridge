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

	"github.com/snowplow-devops/stream-replicator/cmd"
	"github.com/stretchr/testify/assert"
)

// explanation of arguments:
// -i keeps stdin open
// --mount mounts the config file
// --env sets env var for config file resolution
var cmdTemplate = `cat %s | docker run -i \
--name %s \
--net=integration_default \
--mount type=bind,source=%s,target=/config.hcl \
--env STREAM_REPLICATOR_CONFIG_FILE=/config.hcl %s \
snowplow/stream-replicator-aws:` + cmd.AppVersion

// Helper function to run docker command
// This assumes that docker assets are built (make container) and integration resources exist (make integration-up)
func runDockerCommand(cmdTemplate string, containerName string, configFilePath string, additionalOpts string) ([]byte, error) {

	inputFilePath := filepath.Join("input.txt")

	cmdFull := fmt.Sprintf(cmdTemplate, inputFilePath, containerName, configFilePath, additionalOpts)

	fmt.Println(cmdFull)

	cmd := exec.Command("bash", "-c", cmdFull)

	// Ensure we print stderr to logs, to make debugging a bit more manageable
	cmd.Stderr = os.Stderr

	// Goroutine to stop SR after a bit, in case it hangs
	// TODO: we might want to make this configurable.
	go func() {
		time.Sleep(3 * time.Second)
		cmd := exec.Command("bash", "-c", "docker stop "+containerName)
		// Ensure we print stderr to logs, to make debugging a bit more manageable
		cmd.Stderr = os.Stderr
		cmd.Output()
	}()

	out, err := cmd.Output()

	defer func() {
		// Remove container before exiting, existing stopped container will cause next docker run to fail.
		rmCmd := exec.Command("bash", "-c", "docker rm "+containerName)

		rmCmd.Stderr = os.Stderr
		rmCmd.Output()

	}()

	return out, err
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

// Helper function to evaluate tests for String data (TSV and others)
func evaluateTestCaseString(t *testing.T, actual []byte, expectedFilePath string, testCase string) {
	assert := assert.New(t)

	expectedChunk, err := os.ReadFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	foundData := getDataFromResult(actual)

	expectedData := strings.Split(string(expectedChunk), "\n")

	// Check that we got the correct number of results
	assert.Equal(len(expectedData), len(foundData), testCase)

	// Check that the data is equal
	sort.Strings(expectedData)
	sort.Strings(foundData)

	for i, expected := range expectedData {
		assert.Equal(expected, foundData[i], testCase)
	}
}
