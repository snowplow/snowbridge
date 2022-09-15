// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"encoding/base64"
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

// TODO: First pass of these tests only tests that configs 'compile', doesn't test the actual functionality.
// For scripting transformations, we certainly should do so. For builtins, it'd be an advantage but less important.

// Until transformation configs are refactored, we can't do the same checks on full configurations.
// TODO: Refactor transformation config then implement the same null checks as other examples.

func TestBuiltinTransformationDocumentation(t *testing.T) {
	assert := assert.New(t)

	transformationsToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstructEvent", "spEnrichedSetPk", "spEnrichedToJson"}

	for _, tfm := range transformationsToTest {

		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "snowplow-builtin", tfm+".md")

		fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

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

// These are likely to be more complicated/harder to read, so creating a separate function to test teh different parts of the docs.
func TestScriptTransformationCreateAScript(t *testing.T) {
	assert := assert.New(t)

	// Read file:
	markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "custom-scripts", "create-a-script.md")

	fencedHCLBlocksFound, fencedOtherBlocksFound := getFencedBlocksFromMd(markdownFilePath)

	// No HCL, and some other blocks should be in there
	assert.Equal(0, len(fencedHCLBlocksFound))
	assert.NotEqual(0, len(fencedOtherBlocksFound))

	for _, block := range fencedOtherBlocksFound {
		switch block["language"] {
		case "js":
			// Test that all of our JS snippets compile with the engine, pass smoke test, and successfully create a transformation function
			testJSScriptCompiles(t, block["script"])
		case "lua":
			// Test that all of our Lua snippets compile with the engine, pass smoke test, and successfully create a transformation function
			testLuaScriptCompiles(t, block["script"])
		case "go":
			// There is one go example which doesn't need testing

		default:
			// Otherwise it's likely a typo or error.
			assert.Fail("unexpected code block found: %v", block)
		}
	}
}

func testJSScriptCompiles(t *testing.T, script string) {
	assert := assert.New(t)

	src := base64.StdEncoding.EncodeToString([]byte(script))
	jsConfig := &engine.JSEngineConfig{
		SourceB64:  src,
		RunTimeout: 5, // This is needed here as we're providing config directly, not using defaults.
	}

	jsEngine, err := engine.NewJSEngine(jsConfig)
	assert.NotNil(jsEngine)
	if err != nil {
		t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	if err := jsEngine.SmokeTest("main"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	transFunction := jsEngine.MakeFunction("main")

	assert.NotNil(transFunction)
}

// TODO: Make failures easier to debug by providing the scripts themselves when we fail.
func testLuaScriptCompiles(t *testing.T, script string) {
	assert := assert.New(t)

	src := base64.StdEncoding.EncodeToString([]byte(script))
	luaConfig := &engine.LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5, // This is needed here as we're providing config directly, not using defaults.
	}

	luaEngine, err := engine.NewLuaEngine(luaConfig)
	assert.NotNil(luaEngine)
	if err != nil {
		t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	if err := luaEngine.SmokeTest("main"); err != nil {
		t.Fatalf("smoke-test failed with error: %q", err.Error())
	}

	transFunction := luaEngine.MakeFunction("main")
	assert.NotNil(transFunction)
}

func TestScriptTransformationConfigurations(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"javascript", "lua"}

	for _, language := range casesToTest {
		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "custom-scripts", language+"-configuration.md")

		fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound))
		assert.LessOrEqual(len(fencedBlocksFound), 2)
		// TODO: This won't give a very informative error. Fix that.

		for _, block := range fencedBlocksFound {
			c := createConfigFromCodeBlock(t, block)

			// GetTransformations here will run smoke test
			transformFunc, err := transformconfig.GetTransformations(c)

			// For now, we're just testing that the config is valid here
			assert.NotNil(transformFunc)
			assert.Nil(err)
		}
	}
}

func TestScriptTransformationExamples(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"js-non-snowplow", "js-snowplow", "lua-non-snowplow", "lua-snowplow"}

	for _, example := range casesToTest {

		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "custom-scripts", "examples", example+".md")

		fencedBlocksFound, fencedOtherBlocksFound := getFencedBlocksFromMd(markdownFilePath)

		// Test that script code examples compile
		for _, block := range fencedOtherBlocksFound {
			switch block["language"] {
			case "js":
				// Test that all of our JS snippets compile with the engine, pass smoke test, and successfully create a transformation function
				testJSScriptCompiles(t, block["script"])
			case "lua":
				// Test that all of our Lua snippets compile with the engine, pass smoke test, and successfully create a transformation function
				testLuaScriptCompiles(t, block["script"])
			case "json":
				// There is one json example which doesn't need testing
			default:
				// Otherwise it's likely a typo or error.
				assert.Fail("unexpected code block found: %v", block)
			}
		}

		// Test that config examples compile
		for _, block := range fencedBlocksFound {
			c := createConfigFromCodeBlock(t, block)

			// GetTransformations here will run smoke test
			transformFunc, err := transformconfig.GetTransformations(c)

			// For now, we're just testing that the config is valid here
			assert.NotNil(transformFunc)
			assert.Nil(err)
		}
	}
}
