package releasetest

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCheckSpEnrichedToJsonResult TODO do we need a description?
func TestCheckSpEnrichedTSerPkResult(t *testing.T) {
	assert := assert.New(t)

	_, scriptPath, _, _ := runtime.Caller(0)

	scriptDir := filepath.Dir(scriptPath)

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
		pkSplit := strings.Split(foundRow, "PartitionKey:")

		data := strings.Split(pkSplit[1], ",")
		fmt.Println(data)
		foundData = append(foundData, data[0])
	}

	// This doesn't work as order of keys is random.
	// sort.Strings(expectedData)
	// sort.Strings(foundData)

	// janky way around unsorted json and slices...
	sort.Strings(foundData)

	assert.Equal(2, len(foundData))

	assert.Equal([]string{"test-data1", "test-data2"}, foundData)
}
