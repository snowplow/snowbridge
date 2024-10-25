/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package transform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/itchyny/gojq"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// JqCommandOutput is a type representing output after executing JQ command. For filters for example we expect it to be boolean.
type JqCommandOutput = interface{}

// JqOutputHandler is a function which accepts JqCommandOutput and is response for doing something with it. For filters for example that would be filtering message based on boolean output.
type JqOutputHandler func(JqCommandOutput) TransformationFunction

// GojqTransformationFunction is a function returning another transformation function which allows us to do some GOJQ based mapping/filtering. Actual transformation happens in provided JqOutputHandler.
func GojqTransformationFunction(command string, timeoutMs int, spMode bool, jqOutputHandler JqOutputHandler) (TransformationFunction, error) {
	query, err := gojq.Parse(command)
	if err != nil {
		return nil, fmt.Errorf("error parsing jq command: %s", err)
	}

	// epoch converts a time.Time to an epoch in seconds, as integer type.
	// It must be an integer in order to chain with jq-native time functions
	withEpochFunction := gojq.WithFunction("epoch", 0, 1, func(a1 any, a2 []any) any {
		if a1 == nil {
			return nil
		}

		validTime, ok := a1.(time.Time)

		if !ok {
			return errors.New("Not a valid time input to 'epoch' function")
		}

		return int(validTime.Unix())
	})

	// epochMillis converts a time.Time to an epoch in milliseconds
	withEpochMillisFunction := gojq.WithFunction("epochMillis", 0, 1, func(a1 any, a2 []any) any {
		if a1 == nil {
			return nil
		}

		validTime, ok := a1.(time.Time)

		if !ok {
			return errors.New("Not a valid time input to 'epochMillis' function")
		}

		return validTime.UnixMilli()
	})

	code, err := gojq.Compile(query, withEpochMillisFunction, withEpochFunction)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %s", err)
	}

	return runFunction(code, timeoutMs, spMode, jqOutputHandler), nil
}

func runFunction(jqcode *gojq.Code, timeoutMs int, spMode bool, jqOutputHandler JqOutputHandler) TransformationFunction {
	return func(message *models.Message, interState interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
		input, parsedEvent, err := mkJQInput(message, interState, spMode)
		if err != nil {
			message.SetError(err)
			return nil, nil, message, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()

		iter := jqcode.RunWithContext(ctx, input)
		// no looping since we only keep first value
		jqOutput, ok := iter.Next()
		if !ok {
			message.SetError(errors.New("jq query got no output"))
			return nil, nil, message, nil
		}

		if err, ok := jqOutput.(error); ok {
			message.SetError(err)
			return nil, nil, message, nil
		}

		return jqOutputHandler(jqOutput)(message, parsedEvent)
	}
}

// mkJQInput ensures the input to JQ query is of expected type
func mkJQInput(message *models.Message, interState interface{}, spMode bool) (map[string]interface{}, analytics.ParsedEvent, error) {
	if !spMode {
		// gojq input can only be map[string]any or []any
		// here we only consider the first, but we could also expand
		var input map[string]interface{}
		err := json.Unmarshal(message.Data, &input)
		if err != nil {
			return nil, nil, err
		}

		return input, nil, nil
	}

	parsedEvent, err := IntermediateAsSpEnrichedParsed(interState, message)
	if err != nil {
		return nil, nil, err
	}

	spInput, err := parsedEvent.ToMap()
	if err != nil {
		return nil, nil, err
	}

	return spInput, parsedEvent, nil
}
