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

var opaScript2 = `
package snp

default drop := false

drop {
	input.app_id == "test-data3"
	input.contexts_nl_basjes_yauaa_context_1[_].test1.test2[_].test3 == "testValue"
}

drop {
	input.app_id == "test-data1"
}
`

var luaScript2 = `
function main(input)
	local spData = input["Data"]
	local found = false

	for key, value in pairs(spData.contexts_nl_basjes_yauaa_context_1) do
		for key2, value2 in pairs(value.test1.test2) do
				if (value2.test3 == "testValue") then 
				found = true
				end
		end
	end

	if spData["app_id"] == "test-data3" and found then
		return { FilterOut = true }
	end

	if spData["app_id"] == "test-data1" then
		return { FilterOut = true }
	end

	return input
end
`

func BenchmarkOPAFilterComplex(b *testing.B) {
	b.ReportAllocs()

	server, err := sdktest.NewServer(sdktest.MockBundle("/bundles/bundle.tar.gz", map[string]string{
		"example.rego": opaScript2,
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

func BenchmarkLuaFilterComplex(b *testing.B) {
	b.ReportAllocs()

	src := base64.StdEncoding.EncodeToString([]byte(luaScript2))
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

/*
BenchmarkOPAFilterSimple-10       	    7786	    136698 ns/op	   87629 B/op	    1153 allocs/op
BenchmarkJSFilterSimple-10        	    7430	    163486 ns/op	  189904 B/op	    2227 allocs/op
BenchmarkLuaFilterSimple-10       	   12736	     92110 ns/op	  174377 B/op	     623 allocs/op
BenchmarkNativeFilterSimple-10    	  163545	      7183 ns/op	    8406 B/op	      51 allocs/op
BenchmarkOPAFilterComplex-10      	    7615	    135302 ns/op	   86682 B/op	    1145 allocs/op
BenchmarkLuaFilterComplex-10      	   10000	    100731 ns/op	  182560 B/op	     619 allocs/op
*/
