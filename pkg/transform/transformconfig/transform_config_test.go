// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transformconfig

import (
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/engine"
)

func TestMkEngineFunction(t *testing.T) {
	var eng engine.Engine
	eng = &engine.JSEngine{
		Code:       nil,
		RunTimeout: 15,
		SpMode:     false,
	}
	testCases := []struct {
		Name           string
		Engines        []engine.Engine
		Transformation *Transformation
		ExpectedErr    error
	}{
		{
			Name:    "no engine",
			Engines: nil,
			Transformation: &Transformation{
				Name: "js",
			},
			ExpectedErr: fmt.Errorf("could not find engine for transformation"),
		},
		{
			Name:    "success",
			Engines: []engine.Engine{eng},
			Transformation: &Transformation{
				Name:   "js",
				Engine: eng,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			fun, err := MkEngineFunction(tt.Transformation)

			if tt.ExpectedErr != nil {
				assert.Equal(tt.ExpectedErr.Error(), err.Error())
				assert.Nil(fun)
			} else {
				assert.Nil(err)
				assert.NotNil(fun)
			}
		})
	}
}

func TestValidateTransformations(t *testing.T) {
	srcCode := `
function main(x)
  local jsonObj, _ = json.decode(x)
  local result, _ = json.encode(jsonObj)

  return result
end
`
	src := base64.StdEncoding.EncodeToString([]byte(srcCode))

	luaConfig := &engine.LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    false,
	}

	luaEngine, err := engine.NewLuaEngine(luaConfig)
	assert.NotNil(t, luaEngine)
	if err != nil {
		t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	srcCode = `
function notMain(x)
  return x
end
`
	src = base64.StdEncoding.EncodeToString([]byte(srcCode))

	luaConfig = &engine.LuaEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
		Sandbox:    false,
	}

	luaEngineNoMain, err := engine.NewLuaEngine(luaConfig)
	assert.NotNil(t, luaEngineNoMain)
	if err != nil {
		t.Fatalf("function NewLuaEngine failed with error: %q", err.Error())
	}

	srcCode = `
function main(x) {
   return x;
}
`
	src = base64.StdEncoding.EncodeToString([]byte(srcCode))
	jsConfig := &engine.JSEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
	}

	jsEngine, err := engine.NewJSEngine(jsConfig)
	assert.NotNil(t, jsEngine)
	if err != nil {
		t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	srcCode = `
function notMain(x) {
   return x;
}
`
	src = base64.StdEncoding.EncodeToString([]byte(srcCode))
	jsConfig = &engine.JSEngineConfig{
		SourceB64:  src,
		RunTimeout: 5,
	}

	jsEngineNoMain, err := engine.NewJSEngine(jsConfig)
	assert.NotNil(t, jsEngine)
	if err != nil {
		t.Fatalf("function NewJSEngine failed with error: %q", err.Error())
	}

	testCases := []struct {
		Name            string
		Transformations []*Transformation
		ExpectedErrs    []error
	}{
		{
			Name: "invalid name",
			Transformations: []*Transformation{{
				Name: "wrongName",
			}},
			ExpectedErrs: []error{fmt.Errorf("invalid transformation name: wrongName")},
		},
		{
			Name: "spEnrichedSetPk success",
			Transformations: []*Transformation{{
				Name:        "spEnrichedSetPk",
				AtomicField: `app_id`,
			}},
		},
		{
			Name: "spEnrichedSetPk no field",
			Transformations: []*Transformation{{
				Name: "spEnrichedSetPk",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedSetPk, empty atomic field")},
		},
		{
			Name: "spEnrichedFilter success",
			Transformations: []*Transformation{{
				Name:        "spEnrichedFilter",
				AtomicField: "app_id",
				Regex:       "test.+",
			}},
		},
		{
			Name: "spEnrichedFilter regexp does not compile",
			Transformations: []*Transformation{{
				Name:        "spEnrichedFilter",
				AtomicField: "app_id",
				Regex:       "?(?=-)",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilter, regex does not compile. error: error parsing regexp: missing argument to repetition operator: `?`")},
		},
		{
			Name: "spEnrichedFilter empty atomic field",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilter",
				Regex: "test.+",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilter, empty atomic field")},
		},
		{
			Name: "spEnrichedFilter empty regex",
			Transformations: []*Transformation{{
				Name:        "spEnrichedFilter",
				AtomicField: "app_id",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilter, empty regex")},
		},
		{
			Name: "spEnrichedFilterContext success",
			Transformations: []*Transformation{{
				Name:            "spEnrichedFilterContext",
				ContextFullName: "contexts_nl_basjes_yauaa_context_1",
				CustomFieldPath: "test1.test2[0]",
				Regex:           "test.+",
			}},
		},
		{
			Name: "spEnrichedFilterContext regexp does not compile",
			Transformations: []*Transformation{{
				Name:            "spEnrichedFilterContext",
				ContextFullName: "contexts_nl_basjes_yauaa_context_1",
				CustomFieldPath: "test1.test2[0]",
				Regex:           "?(?=-)",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilterContext, regex does not compile. error: error parsing regexp: missing argument to repetition operator: `?`")},
		},
		{
			Name: "spEnrichedFilterContext empty custom field path",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterContext",
				Regex: "test.+",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilterContext, empty context full name"), fmt.Errorf("validation error #0 spEnrichedFilterContext, empty custom field path")},
		},
		{
			Name: "spEnrichedAddMetadata success",
			Transformations: []*Transformation{{
				Name:        "spEnrichedAddMetadata",
				AtomicField: "app_id",
				MetadataKey: "some-key",
			}},
		},
		{
			Name: "spEnrichedAddMetadata empty atomic field",
			Transformations: []*Transformation{{
				Name:        "spEnrichedAddMetadata",
				MetadataKey: "some-key",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedAddMetadata, empty field")},
		},
		{
			Name: "spEnrichedAddMetadata empty metadata key",
			Transformations: []*Transformation{{
				Name:        "spEnrichedAddMetadata",
				AtomicField: "app_id",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedAddMetadata, empty key")},
		},
		{
			Name: "spEnrichedFilterContext empty regex",
			Transformations: []*Transformation{{
				Name:            "spEnrichedFilterContext",
				ContextFullName: "contexts_nl_basjes_yauaa_context_1",
				CustomFieldPath: "test1.test2[0]",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilterContext, empty regex")},
		},
		{
			Name: "spEnrichedFilterUnstructEvent success",
			Transformations: []*Transformation{{
				Name:              "spEnrichedFilterUnstructEvent",
				CustomFieldPath:   "sku",
				Regex:             "test.+",
				UnstructEventName: "add_to_cart",
			}},
		},
		{
			Name: "spEnrichedFilterUnstructEvent regexp does not compile",
			Transformations: []*Transformation{{
				Name:              "spEnrichedFilterUnstructEvent",
				CustomFieldPath:   "sku",
				Regex:             "?(?=-)",
				UnstructEventName: "add_to_cart",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilterUnstructEvent, regex does not compile. error: error parsing regexp: missing argument to repetition operator: `?`")},
		},
		{
			Name: "spEnrichedFilterUnstructEvent empty custom field path and event name",
			Transformations: []*Transformation{{
				Name:  "spEnrichedFilterUnstructEvent",
				Regex: "test.+",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilterUnstructEvent, empty custom field path"), fmt.Errorf("validation error #0 spEnrichedFilterUnstructEvent, empty event name")},
		},
		{
			Name: "spEnrichedFilterUnstructEvent empty regex and event name",
			Transformations: []*Transformation{{
				Name:            "spEnrichedFilterUnstructEvent",
				CustomFieldPath: "sku",
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error #0 spEnrichedFilterUnstructEvent, empty event name"), fmt.Errorf("validation error #0 spEnrichedFilterUnstructEvent, empty regex")},
		},
		{
			Name: "lua success",
			Transformations: []*Transformation{{
				Name:   "lua",
				Engine: luaEngine,
			}},
		},
		{
			Name: "lua main() smoke test failed",
			Transformations: []*Transformation{{
				Name:   "lua",
				Engine: luaEngineNoMain,
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error in lua transformation #0, main() smoke test failed")},
		},
		{
			Name: "js success",
			Transformations: []*Transformation{{
				Name:   "js",
				Engine: jsEngine,
			}},
		},
		{
			Name: "js main() smoke test failed",
			Transformations: []*Transformation{{
				Name:   "js",
				Engine: jsEngineNoMain,
			}},
			ExpectedErrs: []error{fmt.Errorf("validation error in js transformation #0, main() smoke test failed")},
		},
		{
			Name: "multiple validation errors",
			Transformations: []*Transformation{
				{
					Name:   "js",
					Engine: jsEngineNoMain,
				},
				{
					Name:  "spEnrichedFilter",
					Regex: "test.+",
				},
				// a successful transformation mixed in to test transformation counter
				{
					Name: "spEnrichedToJson",
				},
				{
					Name: "spEnrichedSetPk",
				},
			},
			ExpectedErrs: []error{
				fmt.Errorf("validation error in js transformation #0, main() smoke test failed"),
				fmt.Errorf("validation error #1 spEnrichedFilter, empty atomic field"),
				fmt.Errorf("validation error #3 spEnrichedSetPk, empty atomic field"),
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			valErrs := ValidateTransformations(tt.Transformations)
			if tt.ExpectedErrs != nil {
				for idx, valErr := range valErrs {
					assert.Equal(tt.ExpectedErrs[idx].Error(), valErr.Error())
				}
			} else {
				assert.Nil(valErrs)
			}
		})
	}
}

func TestEnginesAndTransformations(t *testing.T) {
	var messageJSCompileErr = &models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}
	messageJSCompileErr.SetError(errors.New(`failed initializing JavaScript runtime: "could not assert as function: \"main\""`))

	testFixPath := "../../../config/test-fixtures"
	testCases := []struct {
		Description        string
		File               string
		ExpectedTransforms []Transformation
		ExpectedMessages   expectedMessages
		CompileErr         string
	}{
		{
			Description: "simple transform success",
			File:        "transform-js-simple.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: "simple transform with js compile error",
			File:        "transform-js-error.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{messageJSCompileErr},
			},
			CompileErr: `SyntaxError`,
		},
		{
			Description: `mixed success`,
			File:        "transform-mixed.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowJSON1Mixed,
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: `mixed success, order test`,
			File:        "transform-mixed-order.hcl",
			// initial app_id should be changed to 1, then if the app_id is 1, it should be changed to 2, then 3
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowJSON1Order,
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: `mixed with error`,
			File:        "transform-mixed-error.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowJSON1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{messageJSCompileErr},
			},
			CompileErr: `SyntaxError`,
		},
		{
			Description: `mixed with filter success`,
			File:        "transform-mixed-filtered.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Description, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(testFixPath, tt.File)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			// get transformations, and run the transformations on the expected messages
			tr, err := GetTransformations(c)
			if tt.CompileErr != `` {
				assert.True(strings.HasPrefix(err.Error(), tt.CompileErr))
				assert.Nil(tr)
				return
			}

			if err != nil {
				t.Fatalf(err.Error())
			}

			result := tr(tt.ExpectedMessages.Before)
			assert.NotNil(result)
			assert.Equal(int(result.ResultCount+result.FilteredCount+result.InvalidCount), len(tt.ExpectedMessages.After))

			// check result for successfully transformed messages
			for idx, resultMessage := range result.Result {
				assert.Equal(resultMessage.Data, tt.ExpectedMessages.After[idx].Data)
			}

			// check errors for invalid messages
			for idx, resultMessage := range result.Invalid {
				assert.Equal(resultMessage.GetError(), tt.ExpectedMessages.After[idx].GetError())
			}

			// check result for transformed messages in case of filtered results
			if result.FilteredCount != 0 {
				assert.NotNil(result.Filtered)
				for idx, resultMessage := range result.Filtered {
					assert.Equal(resultMessage.Data, tt.ExpectedMessages.After[idx].Data)
				}
			}
		})
	}
}

type expectedMessages struct {
	Before []*models.Message
	After  []*models.Message
}

var snowplowTsv1 = []byte(`test-data1	pc	2019-05-10 14:40:37.436	2019-05-10 14:40:35.972	2019-05-10 14:40:35.551	unstruct	e9234345-f042-46ad-b1aa-424464066a33			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				d26822f5-52cc-4292-8f77-14ef6b7a27e2																																									{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0","data":{"sku":"item41","quantity":2,"unitPrice":32.4,"currency":"GBP"}}}																			python-requests/2.21.0																																										2019-05-10 14:40:35.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:35.972	com.snowplowanalytics.snowplow	add_to_cart	jsonschema	1-0-0		`)
var snowplowJSON1 = []byte(`{"app_id":"test-data1","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user\u003cbuilt-in function input\u003e","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)
var snowplowTsv2 = []byte(`test-data2	pc	2019-05-10 14:40:32.392	2019-05-10 14:40:31.105	2019-05-10 14:40:30.218	transaction_item	5071169f-3050-473f-b03f-9748319b1ef2			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				68220ade-307b-4898-8e25-c4c8ac92f1d7																																																		transaction<built-in function input>	item58			35.87	1					python-requests/2.21.0																																										2019-05-10 14:40:30.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:31.105	com.snowplowanalytics.snowplow	transaction_item	jsonschema	1-0-0		`)
var snowplowTsv3 = []byte(`test-data3	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??","test1":{"test2":[{"test3":"testValue"}]}}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`)

var nonSnowplowString = []byte(`not	a	snowplow	event`)

var messages = []*models.Message{
	{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	},
	{
		Data:         snowplowTsv2,
		PartitionKey: "some-key1",
	},
	{
		Data:         snowplowTsv3,
		PartitionKey: "some-key2",
	},
	{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	},
}

// snowplowJSON1 with 3 transformations applied
var snowplowJSON1Mixed = []byte(`Hello:{"app_id":"again","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// snowplowJSON1 with 3 transformations applied, for order test
var snowplowJSON1Order = []byte(`{"app_id":"3","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)
