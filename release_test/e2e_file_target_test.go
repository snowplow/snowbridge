package releasetest_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/cmd"
	releasetest "github.com/snowplow/snowbridge/release_test"
	"github.com/stretchr/testify/assert"
)

// TestE2EFileTarget is the main test function for file target
func TestE2EFileTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	assert := assert.New(t)
	version := cmd.AppVersion

	configFilePath, err := filepath.Abs(filepath.Join("cases", "targets", "file", "config.hcl"))
	if err != nil {
		t.Fatal(err)
	}

	// Create a temporary directory for the test
	tmpDir, err := os.MkdirTemp("", "snowbridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	outputPath := filepath.Join(tmpDir, "output.txt")

	// Update the configuration file with the correct path
	config := fmt.Sprintf(`
target {
  use "file" {
    path = "%s"
    append = false
    permissions = "0644"
  }
}
disable_telemetry = true
`, outputPath)

	if err := os.WriteFile(configFilePath, []byte(config), 0644); err != nil {
		t.Fatal(err)
	}

	// Run the docker command
	_, cmdErr := releasetest.RunDockerCommand(3*time.Second, "fileTarget", configFilePath, version,
		fmt.Sprintf("--mount type=bind,source=%s,target=%s", tmpDir, tmpDir))
	if cmdErr != nil {
		t.Fatal(cmdErr)
	}

	// Read and process the output file
	outputData, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}

	// Read and process the input file for comparison
	inputData, err := os.ReadFile(releasetest.InputFilePath)
	if err != nil {
		t.Fatal(err)
	}

	// Process both input and output data
	expectedLines := strings.Split(strings.TrimSpace(string(inputData)), "\n")
	actualLines := strings.Split(strings.TrimSpace(string(outputData)), "\n")

	// Remove empty lines and sort
	var cleanExpected, cleanActual []string
	for _, line := range expectedLines {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			cleanExpected = append(cleanExpected, trimmed)
		}
	}
	for _, line := range actualLines {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			cleanActual = append(cleanActual, trimmed)
		}
	}

	sort.Strings(cleanExpected)
	sort.Strings(cleanActual)

	// Compare the processed data
	assert.Equal(cleanExpected, cleanActual, "File target output should match input")
}
