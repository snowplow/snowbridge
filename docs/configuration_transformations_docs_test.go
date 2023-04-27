//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package docs

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/transform"
	"github.com/snowplow/snowbridge/pkg/transform/engine"
	"github.com/snowplow/snowbridge/pkg/transform/filter"
	"github.com/snowplow/snowbridge/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

func TestBuiltinTransformationDocumentation(t *testing.T) {
	transformationsToTest := []string{"base64Decode", "base64Encode"}

	for _, tfm := range transformationsToTest {

		minimalConfigPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "builtin", tfm+"-minimal-example.hcl")

		fullConfigPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "builtin", tfm+"-full-example.hcl")

		testTransformationConfig(t, minimalConfigPath, false)

		testTransformationConfig(t, fullConfigPath, true)
	}
}

func TestBuiltinSnowplowTransformationDocumentation(t *testing.T) {
	transformationsToTest := []string{"spEnrichedFilter", "spEnrichedFilterContext", "spEnrichedFilterUnstructEvent", "spEnrichedSetPk", "spEnrichedToJson"}

	for _, tfm := range transformationsToTest {

		minimalConfigPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "snowplow-builtin", tfm+"-minimal-example.hcl")

		fullConfigPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "snowplow-builtin", tfm+"-full-example.hcl")

		testTransformationConfig(t, minimalConfigPath, false)

		testTransformationConfig(t, fullConfigPath, true)
	}
}

// These are likely to be more complicated/harder to read, so creating a separate function to test teh different parts of the docs.
func TestScriptTransformationCustomScripts(t *testing.T) {
	assert := assert.New(t)

	// Set env vars with paths to scripts
	t.Setenv("JS_SCRIPT_PATH", jsScriptPath)

	luaScriptPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts", "create-a-script-filter-example.lua")
	t.Setenv("LUA_SCRIPT_PATH", luaScriptPath)

	jsNonSnowplowScriptPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts", "examples", "js-non-snowplow-script-example.js")
	t.Setenv("JS_NON_SNOWPLOW_SCRIPT_PATH", jsNonSnowplowScriptPath)

	jsSnowplowScriptPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts", "examples", "js-snowplow-script-example.js")
	t.Setenv("JS_SNOWPLOW_SCRIPT_PATH", jsSnowplowScriptPath)

	luaNonSnowplowScriptPath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts", "examples", "lua-script-example.lua")
	t.Setenv("LUA_SCRIPT_EXAMPLE_PATH", luaNonSnowplowScriptPath)

	baseDir := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts")

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

	examplesDir := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts", "examples")

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
			isFull := strings.Contains(file, "full-example")

			testTransformationConfig(t, file, isFull)
		case "":
			// If there's no extension, fail the test.
			assert.Fail("File with no extension found: %v", file)

		default:
			// Otherwise it's likely a typo or error.
			assert.Fail("unexpected file extension found: %v", file)
		}

	}
}

func TestTransformationsOverview(t *testing.T) {
	// Set env var to script path
	t.Setenv("JS_SCRIPT_PATH", jsScriptPath)

	// Read file:
	configFilePath := filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "transformations-overview-example.hcl")

	testTransformationConfig(t, configFilePath, false)
}

func testTransformationConfig(t *testing.T, filepath string, fullExample bool) {
	assert := assert.New(t)

	c := getConfigFromFilepath(t, filepath)

	// Iterate transformations found
	for _, transformation := range c.Data.Transformations {
		use := transformation.Use

		// Pick the config to compare against
		var configObject interface{}
		switch use.Name {
		case "spEnrichedFilter":
			configObject = &filter.AtomicFilterConfig{}
		case "spEnrichedFilterContext":
			configObject = &filter.ContextFilterConfig{}
		case "spEnrichedFilterUnstructEvent":
			configObject = &filter.UnstructFilterConfig{}
		case "spEnrichedSetPk":
			configObject = &transform.SetPkConfig{}
		case "spEnrichedToJson":
			configObject = &transform.EnrichedToJSONConfig{}
		case "base64Decode":
			configObject = &transform.Base64DecodeConfig{}
		case "base64Encode":
			configObject = &transform.Base64EncodeConfig{}
		case "js":
			configObject = &engine.JSEngineConfig{}
		case "lua":
			configObject = &engine.LuaEngineConfig{}
		default:
			assert.Fail(fmt.Sprint("Source not recognised: ", use.Name))
		}
		// DecodeBody parses a hcl Body object into the provided struct.
		// It will fail if the configurations don't match, or if a required argument is missing.
		err := gohcl.DecodeBody(use.Body, config.CreateHclContext(), configObject)
		if err != nil {
			assert.Fail(use.Name, err.Error())
		}

		if fullExample {
			checkComponentForZeros(t, configObject)
		}

		// Finally, build the function to make sure the example compiles
		transformFunc, buildErr := transformconfig.GetTransformations(c, transformconfig.SupportedTransformations)

		// For now, we're just testing that the config is valid here
		assert.NotNil(transformFunc)
		if buildErr != nil {
			assert.Fail(buildErr.Error())
		}
	}
}

func testJSScriptCompiles(t *testing.T, scriptPath string) {
	assert := assert.New(t)

	jsConfig := &engine.JSEngineConfig{
		ScriptPath: scriptPath,
		RunTimeout: 5, // This is needed here as we're providing config directly, not using defaults.
	}

	// JSConfigFunction validates and smoke tests the function, and only returns valid transformation functions.
	jsTransformationFunc, err := engine.JSConfigFunction(jsConfig)
	assert.NotNil(jsTransformationFunc, scriptPath)
	if err != nil {
		t.Fatalf("JSConfigFunction failed with error: %s. Script: %s", err.Error(), string(scriptPath))

	}

}

func testLuaScriptCompiles(t *testing.T, scriptPath string) {
	assert := assert.New(t)

	luaConfig := &engine.LuaEngineConfig{
		ScriptPath: scriptPath,
		RunTimeout: 5, // This is needed here as we're providing config directly, not using defaults.
	}

	// LuaConfigFunction validates and smoke tests the function, and only returns valid transformation functions.
	luaTransformationFunction, err := engine.LuaConfigFunction(luaConfig)
	assert.NotNil(luaTransformationFunction, scriptPath)
	if err != nil {
		t.Fatalf("NewLuaEngine failed with error: %s. Script: %s", err.Error(), string(scriptPath))
	}
}
