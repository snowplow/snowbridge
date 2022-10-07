// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"encoding/base64"
	"io/ioutil"
	"os"
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

		minimalConfigPath := filepath.Join("documentation-examples", "configuration", "transformations", "snowplow-builtin", tfm+"-minimal-example.hcl")

		fullConfigPath := filepath.Join("documentation-examples", "configuration", "transformations", "snowplow-builtin", tfm+"-full-example.hcl")

		minimalConf := getConfigFromFilepath(t, minimalConfigPath)

		transformFunc1, err := transformconfig.GetTransformations(minimalConf)

		// For now, we're just testing that the config is valid here
		assert.NotNil(transformFunc1)
		assert.Nil(err)

		// Hacky workaround for the fact that some only have one - eventually we'll refactor the tests to look more like soruces & targets, once the config issues are dealt with.
		// So a little hack is acceptable for now.
		if tfm == "spEnrichedSetPk" || tfm == "spEnrichedToJson" {
			continue
		}

		fullConf := getConfigFromFilepath(t, fullConfigPath)

		transformFunc2, err := transformconfig.GetTransformations(fullConf)

		// For now, we're just testing that the config is valid here
		assert.NotNil(transformFunc2)
		assert.Nil(err)

	}
}

// These are likely to be more complicated/harder to read, so creating a separate function to test teh different parts of the docs.
func TestScriptTransformationCustomScripts(t *testing.T) {
	assert := assert.New(t)

	assetDir := filepath.Join("documentation-examples", "configuration", "transformations", "custom-scripts")

	filesInDir, err := ioutil.ReadDir(assetDir)
	if err != nil {
		panic(err)
	}
	for _, file := range filesInDir {
		assetPath := filepath.Join(assetDir, file.Name())
		switch filepath.Ext(file.Name()) {
		case ".js":
			// Test that all of our JS snippets compile with the engine, pass smoke test, and successfully create a transformation function
			testJSScriptCompiles(t, assetPath)
		case ".lua":
			// Test that all of our Lua snippets compile with the engine, pass smoke test, and successfully create a transformation function
			testLuaScriptCompiles(t, assetPath)
		case ".hcl":
			// Test that the example hcl block can be parsed to a valid transformFunc
			c := getConfigFromFilepath(t, assetPath)

			transformFunc, err := transformconfig.GetTransformations(c)

			// For now, we're just testing that the config is valid here
			assert.NotNil(transformFunc)
			assert.Nil(err)
		case "":
			// If there's no extension, it should be a directory. If it isn't, fail the test.
			if !file.IsDir() {
				assert.Fail("File with no extension found: %v", assetPath)
			}
		default:
			// Otherwise it's likely a typo or error.
			assert.Fail("unexpected file extension found: %v", assetPath)
		}

	}
}

func testJSScriptCompiles(t *testing.T, scriptPath string) {
	assert := assert.New(t)

	script, err := os.ReadFile(scriptPath)
	if err != nil {
		panic(err)
	}

	src := base64.StdEncoding.EncodeToString(script)
	jsConfig := &engine.JSEngineConfig{
		SourceB64:  src,
		RunTimeout: 5, // This is needed here as we're providing config directly, not using defaults.
	}

	jsEngine, err := engine.NewJSEngine(jsConfig)
	assert.NotNil(jsEngine, script)
	if err != nil {
		t.Fatalf("NewJSEngine failed with error: %s. Script: %s", err.Error(), string(script))

	}

	if err := jsEngine.SmokeTest("main"); err != nil {
		t.Fatalf("smoke-test failed with error: %s. Script: %s", err.Error(), string(script))
	}

	transFunction := jsEngine.MakeFunction("main")
	assert.NotNil(transFunction, script)
}

func testLuaScriptCompiles(t *testing.T, scriptPath string) {
	assert := assert.New(t)

	script, err := os.ReadFile(scriptPath)
	if err != nil {
		panic(err)
	}

	src := base64.StdEncoding.EncodeToString(script)
	luaConfig := &engine.LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5, // This is needed here as we're providing config directly, not using defaults.
	}

	luaEngine, err := engine.NewLuaEngine(luaConfig)
	assert.NotNil(luaEngine, script)
	if err != nil {
		t.Fatalf("NewLuaEngine failed with error: %s. Script: %s", err.Error(), string(script))
	}

	if err := luaEngine.SmokeTest("main"); err != nil {
		t.Fatalf("smoke-test failed with error: %s. Script: %s", err.Error(), string(script))
	}

	transFunction := luaEngine.MakeFunction("main")
	assert.NotNil(transFunction, script)
}

func TestScriptTransformationConfigurations(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"javascript", "lua"}

	for _, language := range casesToTest {
		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "custom-scripts", language+"-configuration.md")

		fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound), "Unexpected number of hcl blocks found")
		assert.LessOrEqual(len(fencedBlocksFound), 2, "Unexpected number of hcl blocks found")

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

func TestTransformationsOverview(t *testing.T) {
	assert := assert.New(t)
	// Read file:
	configFilePath := filepath.Join("documentation-examples", "configuration", "transformations", "transformations-overview-example.hcl")

	c := getConfigFromFilepath(t, configFilePath)

	transformFunc, err := transformconfig.GetTransformations(c)

	// For now, we're just testing that the config is valid here
	assert.NotNil(transformFunc)
	assert.Nil(err)

}

// // TEMP:

//

//

///

//

/*
func TestBuiltinTransformationDocumentationWriteConfigs(t *testing.T) {
	assert := assert.New(t)

	transformationsToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstructEvent", "spEnrichedSetPk", "spEnrichedToJson"}

	for _, tfm := range transformationsToTest {

		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "snowplow-builtin", tfm+".md")

		fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound), "Unexpected number of hcl blocks found")
		assert.LessOrEqual(len(fencedBlocksFound), 2, "Unexpected number of hcl blocks found")

		for i, block := range fencedBlocksFound {
			var typ string
			if i == 0 {
				typ = "-minimal"
			} else {
				typ = "-full"
			}

			configFilePath := filepath.Join("documentation-examples", "configuration", "transformations", "snowplow-builtin", tfm+typ+"-example.hcl")

	}
}

// These are likely to be more complicated/harder to read, so creating a separate function to test teh different parts of the docs.
func TestScriptTransformationCreateAScriptWriteConfigs(t *testing.T) {
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
*/

func TestScriptTransformationConfigurationsWriteConfigs(t *testing.T) {
	assert := assert.New(t)

	casesToTest := []string{"javascript", "lua"}

	for _, language := range casesToTest {
		// Read file:
		markdownFilePath := filepath.Join("documentation", "configuration", "transformations", "custom-scripts", language+"-configuration.md")

		fencedBlocksFound, _ := getFencedBlocksFromMd(markdownFilePath)

		// TODO: perhaps this can be better, but since sometimes we can have one and sometimes two:
		assert.NotEqual(0, len(fencedBlocksFound), "Unexpected number of hcl blocks found")
		assert.LessOrEqual(len(fencedBlocksFound), 2, "Unexpected number of hcl blocks found")

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

func TestScriptTransformationExamplesWriteConfigs(t *testing.T) {
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
