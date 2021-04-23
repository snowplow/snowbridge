package transform

// package main

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// ApplyTransformations applies a set of transformation functions to messages, and outputs a transformationResult
func ApplyTransformations(messages []*models.Message, tranformFunctions ...func([]*models.Message) ([]*models.Message, []*models.Message, error)) (*models.TransformationResult, error) {
	successes := messages
	failures := make([]*models.Message, 0, len(messages))

	for _, transformFunction := range tranformFunctions {
		success, failure, err := transformFunction(messages)
		if err != nil { // TODO: Figure out error handling...
			// do something
		}
		failures = append(failures, failure...)
		successes = success
	}
	return models.NewTransformationResult(successes, failures), nil // TODO: Figure out error handling...
} // This seems generic enough that perhaps it should live elsewhere, if we were to create a set of transformations on raw data or some other format, for example, it could be used.

// EnrichedToJson is a specific transformation implementation to transform good enriched data within a message to Json
func EnrichedToJson(messages []*models.Message) ([]*models.Message, []*models.Message, error) {
	successes := make([]*models.Message, 0, len(messages))
	failures := make([]*models.Message, 0, len(messages))

	for _, message := range messages {
		parsedMessage, err := analytics.ParseEvent(string(message.Data))
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
		}
		JsonMessage, err := parsedMessage.ToJson()
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
		} else {
			message.Data = JsonMessage
			successes = append(successes, message)
		}
	}
	return successes, failures, nil // TO DO: Figure out error handling...
}

// Just using this for quick tests
/*
func main() {

	messages := []*models.Message{
		{
			Data: []byte(`test-data	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`),
			PartitionKey: "some-key",
		},
		{
			Data: []byte(`test-data	pc	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`),
			PartitionKey: "some-key",
		},
		{ // Format broken:
			Data: []byte(`	2019-05-10 14:40:30.836	2019-05-10 14:40:29.576	2019-05-10 14:40:29.204	page_view	e8aef68d-8533-45c6-a672-26a0f01be9bd			py-0.8.2	ssc-0.15.0-googlepubsub	beam-enrich-0.2.0-common-0.36.0	user<built-in function input>	18.194.133.57				b66c4a12-8584-4c7a-9a5d-7c96f59e2556												www.demo-site.com/campaign-landing-page	landing-page				80	www.demo-site.com/campaign-landing-page																																										python-requests/2.21.0																																										2019-05-10 14:40:29.000			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:nl.basjes/yauaa_context/jsonschema/1-0-0","data":{"deviceBrand":"Unknown","deviceName":"Unknown","operatingSystemName":"Unknown","agentVersionMajor":"2","layoutEngineVersionMajor":"??","deviceClass":"Unknown","agentNameVersionMajor":"python-requests 2","operatingSystemClass":"Unknown","layoutEngineName":"Unknown","agentName":"python-requests","agentVersion":"2.21.0","layoutEngineClass":"Unknown","agentNameVersion":"python-requests 2.21.0","operatingSystemVersion":"??","agentClass":"Special","layoutEngineVersion":"??"}}]}		2019-05-10 14:40:29.576	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0		`),
			PartitionKey: "some-key",
		},
	}

	funcs := make([]func([]*models.Message) ([]*models.Message, []*models.Message, error), 0, 0)
	funcs = append(funcs, EnrichedToJson)

	out, _ := ApplyTransformations(messages, funcs...)
	fmt.Println(out)
}
*/
