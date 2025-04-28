/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package transformconfig

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/models"
)

func TestGetTransformations(t *testing.T) {
	assert := assert.New(t)

	// Get absolute paths to test resources
	jsScriptPath := filepath.Join(assets.AssetsRootDir, "test", "transformconfig", "TestGetTransformations", "scripts", "script.js")
	configPath := filepath.Join(assets.AssetsRootDir, "test", "transformconfig", "TestGetTransformations", "configs")

	t.Setenv("JS_SCRIPT_PATH", jsScriptPath)

	// this function executes each test case
	testConfig := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			panic(err)
		}

		if info.IsDir() {
			return nil
		}

		t.Setenv("SNOWBRIDGE_CONFIG_FILE", path)

		c, err := config.NewConfig()
		assert.NotNil(c)
		if err != nil {
			t.Fatalf("function NewConfig failed with error: %q", err.Error())
		}

		// get transformations, and run the transformations on the expected messages
		tr, err := GetTransformations(c, SupportedTransformations)

		// To test the config happy path, we just need to verify that a transformation function is produced, and there's no error.
		assert.NotNil(tr)
		if err != nil {
			assert.Fail(err.Error())
		}

		return err
	}

	// Walk iterates the directory & executes the function.
	filepath.Walk(configPath, testConfig)
}

func TestEnginesAndTransformations(t *testing.T) {
	var messageJSCompileErr = &models.Message{
		Data:         snowplowTsv1,
		PartitionKey: "some-key",
	}
	messageJSCompileErr.SetError(errors.New(`failed initializing JavaScript runtime: "could not assert as function: \"main\""`))

	configDirPath := filepath.Join(assets.AssetsRootDir, "test", "transformconfig", "TestEnginesAndTransformations", "configs")
	scriptDirPath := filepath.Join(assets.AssetsRootDir, "test", "transformconfig", "TestEnginesAndTransformations", "scripts")
	testCases := []struct {
		Description      string
		File             string
		ExpectedMessages expectedMessages
		CompileErr       string
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
			CompileErr: `error building JS engine:`,
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
			CompileErr: `error building JS engine:`,
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
		{
			Description: `e2e latency metric enabled -> collector tstamp attached`,
			File:        "transform-collector-tstamp.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:            snowplowTsv1,
					PartitionKey:    "some-key",
					CollectorTstamp: time.Date(2019, 5, 10, 14, 40, 35, 972000000, time.UTC),
				}},
			},
		},
	}

	// Absolute paths to scripts
	JSPassThroughPath := filepath.Join(scriptDirPath, "js-passthrough.js")
	t.Setenv("JS_PASSTHROUGH_PATH", JSPassThroughPath)

	JSParseJSONPath := filepath.Join(scriptDirPath, "js-json-parse.js")
	t.Setenv("JS_PARSE_JSON_PATH", JSParseJSONPath)

	JSAlterAID1Path := filepath.Join(scriptDirPath, "js-alter-aid-1.js")
	t.Setenv("JS_ALTER_AID_1_PATH", JSAlterAID1Path)

	JSAlterAID2Path := filepath.Join(scriptDirPath, "js-alter-aid-2.js")
	t.Setenv("JS_ALTER_AID_2_PATH", JSAlterAID2Path)

	JSOrderTest1 := filepath.Join(scriptDirPath, "js-order-test-1.js")
	t.Setenv("JS_ORDER_TEST_1", JSOrderTest1)

	JSOrderTest2 := filepath.Join(scriptDirPath, "js-order-test-2.js")
	t.Setenv("JS_ORDER_TEST_2", JSOrderTest2)

	JSOrderTest3 := filepath.Join(scriptDirPath, "js-order-test-3.js")
	t.Setenv("JS_ORDER_TEST_3", JSOrderTest3)

	JSErrorPath := filepath.Join(scriptDirPath, "js-error.txt")
	t.Setenv("JS_ERROR_PATH", JSErrorPath)

	for _, tt := range testCases {
		t.Run(tt.Description, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(configDirPath, tt.File)
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			// get transformations, and run the transformations on the expected messages
			tr, err := GetTransformations(c, SupportedTransformations)
			if tt.CompileErr != `` {
				assert.True(strings.HasPrefix(err.Error(), tt.CompileErr))
				assert.Nil(tr)
				return
			}

			if err != nil {
				t.Fatal(err.Error())
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

			// check if collector timestamp has been attached
			for idx, resultMessage := range result.Result {
				assert.Equal(resultMessage.CollectorTstamp, tt.ExpectedMessages.After[idx].CollectorTstamp)
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

func TestJQHashTransformation(t *testing.T) {

	t.Setenv("SHA1_SALT", "09a2d6b3ecd943aa8512df1f")

	configDirPath := filepath.Join(assets.AssetsRootDir, "test", "transformconfig", "TestEnginesAndTransformations", "configs")

	testCases := []struct {
		Description      string
		File             string
		ExpectedMessages expectedMessages
		CompileErr       string
	}{
		{
			Description: "simple JQ transform with hash - success",
			File:        "transform-jq-hash-function.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         []byte(`{"agentName":"d878ebbdc6fa17d8d0f353a104e0588eac755cc3df18d3e3"}`),
					PartitionKey: "some-key",
				}},
			},
		},
		{
			Description: "simple JQ transform with hash & salt - success",
			File:        "transform-jq-hash-salt-function.hcl",
			ExpectedMessages: expectedMessages{
				Before: []*models.Message{{
					Data:         snowplowTsv1,
					PartitionKey: "some-key",
				}},
				After: []*models.Message{{
					Data:         []byte(`{"agentName":"5841e55de6c4486fa092f044a5189570dec421cb06652829"}`),
					PartitionKey: "some-key",
				}},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Description, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(configDirPath, tt.File)
			t.Setenv("SNOWBRIDGE_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			// get transformations, and run the transformations on the expected messages
			tr, err := GetTransformations(c, SupportedTransformations)
			if tt.CompileErr != `` {
				assert.True(strings.HasPrefix(err.Error(), tt.CompileErr))
				assert.Nil(tr)
				return
			}

			if err != nil {
				t.Fatal(err.Error())
			}

			result := tr(tt.ExpectedMessages.Before)
			assert.NotNil(result)

			assert.Equal(len(tt.ExpectedMessages.After), int(result.ResultCount))

			// check result for successfully transformed messages
			for idx, resultMessage := range result.Result {
				assert.Equal(tt.ExpectedMessages.After[idx].Data, resultMessage.Data)
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
var snowplowJSON1Mixed = []byte(`{"app_id":"again","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// snowplowJSON1 with 3 transformations applied, for order test
var snowplowJSON1Order = []byte(`{"app_id":"3","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user<built-in function input>","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)
