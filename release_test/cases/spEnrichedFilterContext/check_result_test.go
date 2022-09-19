package releasetest

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCheckSpEnrichedToJsonResult TODO do we need a description?

// TODO: There has to be a better way to factor these
func TestCheckSpEnrichedFilterResult(t *testing.T) {
	assert := assert.New(t)

	_, scriptPath, _, _ := runtime.Caller(0)

	scriptDir := filepath.Dir(scriptPath)

	// fmt.Println(scriptDir)

	// Jank way for spike purposes - although this jank might be sufficient if we do use go.
	expectedChunk, err := os.ReadFile(scriptDir + "/expected_data.txt")
	if err != nil {
		panic(err)
	}
	expectedData := strings.Split(string(expectedChunk), "\n")

	// fmt.Print(expectedData)

	foundChunk, err := os.ReadFile(scriptDir + "/result.txt") // 'output' is potentially a better name - technically this function gives us the result, based on the output.
	if err != nil {
		panic(err)
	}

	// stringify and trim trailing newline
	chunkAsString := strings.TrimSuffix(string(foundChunk), "\n")

	foundResults := strings.Split(chunkAsString, "\n")

	// fmt.Print(foundResults)

	var foundData []string

	for _, foundRow := range foundResults {
		data := strings.Split(foundRow, ",Data:")
		foundData = append(foundData, data[len(data)-1])
	}

	// This doesn't work as order of keys is random.
	// sort.Strings(expectedData)
	// sort.Strings(foundData)

	// janky way around unsorted json and slices...
	sort.Slice(expectedData, func(i, j int) bool {
		return len(expectedData[i]) < len(expectedData[j])
	})

	sort.Slice(foundData, func(i, j int) bool {
		return len(foundData[i]) < len(foundData[j])
	})

	assert.Equal(len(expectedData), len(foundData))

	for i, expected := range expectedData {
		// fmt.Println(foundData)
		assert.Equal(expected, foundData[i])
	}
}
