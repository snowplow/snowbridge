// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package engine

import (
	"encoding/base64"
	"fmt"
	"testing"

	sdktest "github.com/open-policy-agent/opa/sdk/test"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

var opaScript1 = `
package snp

default drop := false

drop {
	input.app_id != "sesame"
}
`

var jsScript1 = `
function main(input) {
	// input is an object
	var spData = input.Data;
	if (spData["app_id"] === "sesame") {
		return input;
	}
	return {
		FilterOut: true
	};
 }
`

var luaScript1 = `
function main(input)
  -- input is a lua table
  local spData = input["Data"]
  if spData["app_id"] == "sesame" then
     return input;
  end
  return { FilterOut = true }
end
`

func BenchmarkOPAFilterSimple(b *testing.B) {
	b.ReportAllocs()

	server, err := sdktest.NewServer(sdktest.MockBundle("/bundles/bundle.tar.gz", map[string]string{
		"example.rego": opaScript1,
	}))
	if err != nil {
		panic(err)
	}

	defer server.Stop()

	config := fmt.Sprintf(`{
		"services": {
			"test": {
        "url": %q
			}
		},
		"bundles": {
			"test": {
				"resource": "/bundles/bundle.tar.gz"
			}
		},
		"decision_logs": {
			"console": false
		}
	}`, server.URL())

	opa, err := NewOPAEngine(&OPAEngineConfig{OPAConfig: config})
	if err != nil {
		panic(err)
	}

	opaFunc1 := opa.MakeFunction()

	for n := 0; n < b.N; n++ {
		opaFunc1(messages[0], nil)
	}
}

func BenchmarkJSFilterSimple(b *testing.B) {
	b.ReportAllocs()

	src := base64.StdEncoding.EncodeToString([]byte(jsScript1))
	jsConfig := &JSEngineConfig{
		SourceB64:         src,
		RunTimeout:        5,
		DisableSourceMaps: true,
		SpMode:            true,
	}

	jsEngine, err := NewJSEngine(jsConfig)
	if err != nil {
		panic(err)
	}

	if err := jsEngine.SmokeTest("main"); err != nil {
		panic(err)
	}

	transFunction := jsEngine.MakeFunction("main")

	for n := 0; n < b.N; n++ {
		transFunction(messages[0], nil)
	}
}

func BenchmarkLuaFilterSimple(b *testing.B) {
	b.ReportAllocs()

	src := base64.StdEncoding.EncodeToString([]byte(luaScript1))
	luaConfig := &LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 1,
		Sandbox:    true,
		SpMode:     true,
	}

	luaEngine, err := NewLuaEngine(luaConfig)

	if err != nil {
		panic(err)
	}

	if err := luaEngine.SmokeTest(`main`); err != nil {
		panic(err)
	}

	transFunction := luaEngine.MakeFunction(`main`)

	for n := 0; n < b.N; n++ {
		transFunction(messages[0], nil)
	}
}

func BenchmarkNativeFilterSimple(b *testing.B) {
	b.ReportAllocs()

	aidFilterFuncKeep, err := transform.NewSpEnrichedFilterFunction("app_id", "sesame", 10)
	if err != nil {
		panic(err)
	}

	for n := 0; n < b.N; n++ {
		aidFilterFuncKeep(messages[0], nil)
	}
}
