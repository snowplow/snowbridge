// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

// Until transformation configs are refactored, we can't do the same checks on full configurations.
// TODO: Refactor transformation config then implement the same null checks as other examples.
func TestBuiltinTransformationDocumentation(t *testing.T) {
	assert := assert.New(t)

	transformationsToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstructEvent", "spEnrichedSetPk", "spEnrichedToJson"}

	for _, tfm := range transformationsToTest {

		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "snowplow-builtin", tfm+".md")

		fencedBlocksFound := getFencedHCLBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound))
		assert.LessOrEqual(len(fencedBlocksFound), 2)
		// TODO: This won't give a very informative error. Fix that.

		for _, block := range fencedBlocksFound {
			c := createConfigFromCodeBlock(t, block)

			transformFunc, err := transformconfig.GetTransformations(c)

			// For now, we're just testing that the config is valid here
			assert.NotNil(transformFunc)
			assert.Nil(err)

		}
	}
}

/*

func TestTargetDocumentation(t *testing.T) {
	assert := assert.New(t)

	// Set env vars referenced in the config examples
	t.Setenv("MY_AUTH_PASSWORD", "test")
	t.Setenv("SASL_PASSWORD", "test")

	targetsToTest := []string{"eventhub", "http", "kafka", "kinesis", "pubsub", "sqs"}

	for _, tgt := range targetsToTest {

		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "targets", tgt+".md")

		fencedBlocksFound := getFencedHCLBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound))
		assert.LessOrEqual(len(fencedBlocksFound), 2)
		// TODO: This won't give a very informative error. Fix that.

		// Sort by length to determine which is the minimal example.
		sort.Slice(fencedBlocksFound, func(i, j int) bool {
			return len(fencedBlocksFound[i]) < len(fencedBlocksFound[j])
		})

		// Test minimal config
		// Shortest is always minimal
		testMinimalTargetConfig(t, fencedBlocksFound[0])
		// Test full config
		// Longest is the full config. Where there are no required arguments, there is only one config.
		// In that scenario, both tests should pass.
		testFullTargetConfig(t, fencedBlocksFound[len(fencedBlocksFound)-1])
	}

}
*/
