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

		// Hacky workaround for the fact that some only have one config - eventually we'll refactor the tests to look more like soruces & targets, once the config issues are dealt with.
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

	baseDir := filepath.Join("documentation-examples", "configuration", "transformations", "custom-scripts")

	filesInBaseDir, err := ioutil.ReadDir(baseDir)
	if err != nil {
		panic(err)
	}

	filesToTest := make([]string, 0)
	for _, file := range filesInBaseDir {
		if !file.IsDir() {
			filesToTest = append(filesToTest, filepath.Join(baseDir, file.Name()))
		}
	}

	examplesDir := filepath.Join("documentation-examples", "configuration", "transformations", "custom-scripts", "examples")

	filesInDir, err := ioutil.ReadDir(examplesDir)
	if err != nil {
		panic(err)
	}

	for _, file := range filesInDir {
		if !file.IsDir() {
			filesToTest = append(filesToTest, filepath.Join(examplesDir, file.Name()))
		}
	}

	for _, file := range filesToTest {
		switch filepath.Ext(file) {
		case ".js":
			// Test that all of our JS snippets compile with the engine, pass smoke test, and successfully create a transformation function
			testJSScriptCompiles(t, file)
		case ".lua":
			// Test that all of our Lua snippets compile with the engine, pass smoke test, and successfully create a transformation function
			testLuaScriptCompiles(t, file)
		case ".hcl":
			// Test that the example hcl block can be parsed to a valid transformFunc
			c := getConfigFromFilepath(t, file)

			transformFunc, err := transformconfig.GetTransformations(c)

			// For now, we're just testing that the config is valid here
			assert.NotNil(transformFunc)
			assert.Nil(err)
		case "":
			// If there's no extension, fail the test.

			assert.Fail("File with no extension found: %v", file)

		default:
			// Otherwise it's likely a typo or error.
			assert.Fail("unexpected file extension found: %v", file)
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
