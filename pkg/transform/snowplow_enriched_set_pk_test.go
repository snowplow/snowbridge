// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
	"github.com/stretchr/testify/assert"
)

func TestNewSpEnrichedSetPkFunction(t *testing.T) {
	assert := assert.New(t)

	// Handling of test inputs is messy but avoids edit-in-place complications. Perhaps there's a cleaner way?
	var message1 = models.Message{
		Data: []byte(`test-data	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`),
		PartitionKey: "some-key",
	}

	var message2 = models.Message{
		Data: []byte(`not	a	snowplow	event`),
		PartitionKey: "some-key4",
	}

	aidSetPkFunc := NewSpEnrichedSetPkFunction("app_id")

	stringAsPk, fail := aidSetPkFunc(&message1)

	assert.Equal("test-data", stringAsPk.PartitionKey)
	assert.Nil(fail)

	ctstampSetPkFunc := NewSpEnrichedSetPkFunction("collector_tstamp")

	tstampAsPk, fail := ctstampSetPkFunc(&message1)

	assert.Equal("2019-05-10 14:40:29.576 +0000 UTC", tstampAsPk.PartitionKey)
	assert.Nil(fail)

	pgurlportSetPkFunc := NewSpEnrichedSetPkFunction("page_urlport")

	intAsPk, failure := pgurlportSetPkFunc(&message1)

	assert.Equal("80", intAsPk.PartitionKey)
	assert.Nil(failure)

	// TODO: tests for other types?

	failureCase, fail := aidSetPkFunc(&message2)

	assert.Nil(failureCase)
	assert.NotNil(fail)
	assert.Equal("Cannot parse tsv event - wrong number of fields provided: 20", fail.GetError().Error())
	// Error message to be updated after fix in analytics sdk
}

func TestNewSpEnrichedSetPkFunction_WithIntermediateState(t *testing.T) {
	assert := assert.New(t)

	tsvEvent := []byte(`test-data	pc	2019-05-10 14:40:37.436	2019-05-10 14:40:35.972	2019-05-10 14:40:35.551	unstruct	e9234345-f042-46ad-b1aa-424464066a33			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				d26822f5-52cc-4292-8f77-14ef6b7a27e2																																									{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/add_to_cart/jsonschema/1-0-0","data":{"sku":"item41","quantity":2,"unitPrice":32.4,"currency":"GBP"}}}																			python-requests/2.21.0																																										2019-05-10 14:40:35.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:35.972	com.snowplowanalytics.snowplow	add_to_cart	jsonschema	1-0-0		`)
	parsed, _ := analytics.ParseEvent(string(tsvEvent))

	message := models.Message{
		Data:              tsvEvent,
		PartitionKey:      "some-key",
		IntermediateState: parsed,
	}

	expected := models.Message{
		Data:              tsvEvent,
		PartitionKey:      "some-key",
		IntermediateState: parsed,
	}

	aidSetPkFunc := NewSpEnrichedSetPkFunction("app_id")

	stringAsPk, fail := aidSetPkFunc(&message)

	assert.Equal("test-data", stringAsPk.PartitionKey)
	assert.Equal(expected.Data, stringAsPk.Data)
	assert.Equal(expected.IntermediateState, stringAsPk.IntermediateState)
	assert.Nil(fail)

	incompatibleIntermediateMessage := models.Message{
		Data:              tsvEvent,
		PartitionKey:      "some-key",
		IntermediateState: "Incompatible intermediate state",
	}

	stringAsPkIncompat, failIncompat := aidSetPkFunc(&incompatibleIntermediateMessage)
	assert.Equal("test-data", stringAsPkIncompat.PartitionKey)
	assert.Equal(expected.Data, stringAsPkIncompat.Data)
	assert.Equal(expected.IntermediateState, stringAsPkIncompat.IntermediateState)
	assert.Nil(failIncompat)
}
