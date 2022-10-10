// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// SnowplowTsv1 is test data
var SnowplowTsv1 = []byte(`test-data1	pc	2019-05-10 14:40:37.436	2019-05-10 14:40:35.972	2019-05-10 14:40:35.551	unstruct	e9234345-f042-46ad-b1aa-424464066a33			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				d26822f5-52cc-4292-8f77-14ef6b7a27e2																																									{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0","data":{"sku":"item41","quantity":2,"unitPrice":32.4,"currency":"GBP"}}}																			python-requests/2.21.0																																										2019-05-10 14:40:35.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.acme/justInts/jsonschema/1-0-0", "data":{"integerField": 0}},{"schema":"iglu:com.acme/justInts/jsonschema/1-0-0", "data":{"integerField": 1}},{"schema":"iglu:com.acme/justInts/jsonschema/1-0-0", "data":{"integerField": 2}},{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:35.972	com.snowplowanalytics.snowplow	add_to_cart	jsonschema	1-0-0		`)

// SpTsv1Parsed is test data
var SpTsv1Parsed, _ = analytics.ParseEvent(string(SnowplowTsv1))
var snowplowJSON1 = []byte(`{"app_id":"test-data1","collector_tstamp":"2019-05-10T14:40:35.972Z","contexts_com_acme_just_ints_1":[{"integerField":0},{"integerField":1},{"integerField":2}],"contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:35.972Z","dvce_created_tstamp":"2019-05-10T14:40:35.551Z","dvce_sent_tstamp":"2019-05-10T14:40:35Z","etl_tstamp":"2019-05-10T14:40:37.436Z","event":"unstruct","event_format":"jsonschema","event_id":"e9234345-f042-46ad-b1aa-424464066a33","event_name":"add_to_cart","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"d26822f5-52cc-4292-8f77-14ef6b7a27e2","platform":"pc","unstruct_event_com_snowplowanalytics_snowplow_add_to_cart_1":{"currency":"GBP","quantity":2,"sku":"item41","unitPrice":32.4},"user_id":"user\u003cbuilt-in function input\u003e","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// SnowplowTsv2 is test data
var SnowplowTsv2 = []byte(`test-data2	pc	2019-05-10 14:40:32.392	2019-05-10 14:40:31.105	2019-05-10 14:40:30.218	transaction_item	5071169f-3050-473f-b03f-9748319b1ef2			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				68220ade-307b-4898-8e25-c4c8ac92f1d7																																																		transaction<built-in function input>	item58			35.87	1					python-requests/2.21.0																																										2019-05-10 14:40:30.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:31.105	com.snowplowanalytics.snowplow	transaction_item	jsonschema	1-0-0		`)

// SpTsv2Parsed is test data
var SpTsv2Parsed, _ = analytics.ParseEvent(string(SnowplowTsv2))
var snowplowJSON2 = []byte(`{"app_id":"test-data2","collector_tstamp":"2019-05-10T14:40:31.105Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??"}],"derived_tstamp":"2019-05-10T14:40:31.105Z","dvce_created_tstamp":"2019-05-10T14:40:30.218Z","dvce_sent_tstamp":"2019-05-10T14:40:30Z","etl_tstamp":"2019-05-10T14:40:32.392Z","event":"transaction_item","event_format":"jsonschema","event_id":"5071169f-3050-473f-b03f-9748319b1ef2","event_name":"transaction_item","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"68220ade-307b-4898-8e25-c4c8ac92f1d7","platform":"pc","ti_orderid":"transaction\u003cbuilt-in function input\u003e","ti_price":35.87,"ti_quantity":1,"ti_sku":"item58","user_id":"user\u003cbuilt-in function input\u003e","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// SnowplowTsv3 is test data
var SnowplowTsv3 = []byte(`test-data3	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??","test1":{"test2":[{"test3":"testValue"}]}}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`)

// SpTsv3Parsed is test data
var SpTsv3Parsed, _ = analytics.ParseEvent(string(SnowplowTsv3))
var snowplowJSON3 = []byte(`{"app_id":"test-data3","collector_tstamp":"2019-05-10T14:40:29.576Z","contexts_nl_basjes_yauaa_context_1":[{"agentClass":"Special","agentName":"python-requests","agentNameVersion":"python-requests 2.21.0","agentNameVersionMajor":"python-requests 2","agentVersion":"2.21.0","agentVersionMajor":"2","deviceBrand":"Unknown","deviceClass":"Unknown","deviceName":"Unknown","layoutEngineClass":"Unknown","layoutEngineName":"Unknown","layoutEngineVersion":"??","layoutEngineVersionMajor":"??","operatingSystemClass":"Unknown","operatingSystemName":"Unknown","operatingSystemVersion":"??","test1":{"test2":[{"test3":"testValue"}]}}],"derived_tstamp":"2019-05-10T14:40:29.576Z","dvce_created_tstamp":"2019-05-10T14:40:29.204Z","dvce_sent_tstamp":"2019-05-10T14:40:29Z","etl_tstamp":"2019-05-10T14:40:30.836Z","event":"page_view","event_format":"jsonschema","event_id":"e8aef68d-8533-45c6-a672-26a0f01be9bd","event_name":"page_view","event_vendor":"com.snowplowanalytics.snowplow","event_version":"1-0-0","network_userid":"b66c4a12-8584-4c7a-9a5d-7c96f59e2556","page_title":"landing-page","page_url":"www.demo-site.com/campaign-landing-page","page_urlpath":"www.demo-site.com/campaign-landing-page","page_urlport":80,"platform":"pc","user_id":"user\u003cbuilt-in function input\u003e","user_ipaddress":"18.194.133.57","useragent":"python-requests/2.21.0","v_collector":"ssc-0.15.0-googlepubsub","v_etl":"beam-enrich-0.2.0-common-0.36.0","v_tracker":"py-0.8.2"}`)

// SnowplowTsv4 is test data
var SnowplowTsv4 = []byte(`test-data3	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??","test1":{"test2":[{"test3":1}]}}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`)
var nonSnowplowString = []byte(`not	a	snowplow	event`)

// Messages is test data
var Messages = []*models.Message{
	{
		Data:         SnowplowTsv1,
		PartitionKey: "some-key",
	},
	{
		Data:         SnowplowTsv2,
		PartitionKey: "some-key1",
	},
	{
		Data:         SnowplowTsv3,
		PartitionKey: "some-key2",
	},
	{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	},
}
