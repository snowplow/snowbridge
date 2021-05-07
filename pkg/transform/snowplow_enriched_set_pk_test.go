// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestNewSpEnrichedSetPkFunction(t *testing.T) {
	assert := assert.New(t)

	// Handling of test inputs is messy but avoids edit-in-place complications. Perhaps there's a cleaner way?
	var message1 = models.Message{
		Data: []byte(`test-data	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`),
		PartitionKey: "some-key",
	}
	/*
		var message2 = models.Message{
			Data: []byte(`not	a	snowplow	event`),
			PartitionKey: "some-key4",
		}
	*/

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
}
