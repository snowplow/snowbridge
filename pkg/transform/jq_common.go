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
type JqCommandOutput = any

// JqOutputHandler is a function which accepts JqCommandOutput and is response for doing something with it.
// For filters for example that would be filtering message based on boolean output.
type JqOutputHandler func(JqCommandOutput) TransformationFunction

// GojqTransformationFunction is a function returning another transformation function which allows us to do some GOJQ based mapping/filtering.
// Actual transformation happens in provided JqOutputHandler.
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

		validTime, err := parseTime(a1, a2)
		if err != nil {
			return err
		}

		return int(validTime.Unix())
	})

	// epochMillis converts a time.Time to an epoch in milliseconds
	withEpochMillisFunction := gojq.WithFunction("epochMillis", 0, 1, func(a1 any, a2 []any) any {
		if a1 == nil {
			return nil
		}

		validTime, err := parseTime(a1, a2)
		if err != nil {
			return err
		}
		return validTime.UnixMilli()
	})

	// hash takes a string and applies selected hash function to it
	withHashFunction := gojq.WithFunction("hash", 0, 2, func(a1 any, a2 []any) any {
		if a1 == nil {
			return nil
		}

		hashedValue, err := resolveHash(a1, a2)
		if err != nil {
			return err
		}

		return hashedValue
	})

	code, err := gojq.Compile(query, withEpochMillisFunction, withEpochFunction, withHashFunction)
	if err != nil {
		return nil, fmt.Errorf("error compiling jq query: %s", err)
	}

	return runFunction(code, timeoutMs, spMode, jqOutputHandler), nil
}

func parseTime(input any, params []any) (time.Time, error) {
	switch v := input.(type) {
	case string:
		timeLayout, err := parseTimeLayout(params)
		if err != nil {
			return time.Time{}, err
		}

		validTime, err := time.Parse(timeLayout, v)
		if err != nil {
			return time.Time{}, fmt.Errorf("could not parse input - '%s' using provided time layout - '%s'", v, timeLayout)
		}
		return validTime, nil
	case time.Time:
		return v, nil
	default:
		return time.Time{}, fmt.Errorf("not a valid time input to 'epochMillis' function - '%v'; expected string or time.Time", input)
	}
}

func parseTimeLayout(params []any) (string, error) {
	if len(params) == 0 {
		return "2006-01-02T15:04:05.999Z", nil
	} else if len(params) == 1 {
		str, ok := params[0].(string)
		if !ok {
			return "", fmt.Errorf("function argument is invalid '%v'; expected string", params[0])
		}
		return str, nil
	} else {
		return "", fmt.Errorf("too many function arguments - %d; expected 1", len(params))
	}
}

func resolveHash(input any, params []any) (string, error) {
	inputString, ok := input.(string)
	if !ok {
		return "", fmt.Errorf("hash function input must be a string")
	}

	if len(params) != 2 {
		return "", fmt.Errorf("[%d] parameters given, hash function expecting 2: hash function name and salt", len(params))
	}

	hashFunctionName := params[0].(string)
	hashSalt := params[1].(string)

	return DoHashing(inputString, hashFunctionName, hashSalt)
}

func runFunction(jqcode *gojq.Code, timeoutMs int, spMode bool, jqOutputHandler JqOutputHandler) TransformationFunction {
	return func(message *models.Message, interState any) (*models.Message, *models.Message, *models.Message, any) {
		input, parsedEvent, err := mkJQInput(message, interState, spMode)
		if err != nil {
			message.SetError(&models.TransformationError{
				SafeMessage: "failed to prepare expected JQ input",
				Err:         err,
			})
			return nil, nil, message, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
		defer cancel()

		iter := jqcode.RunWithContext(ctx, input)
		// no looping since we only keep first value
		jqOutput, ok := iter.Next()
		if !ok {
			err := errors.New("jq query got no output")
			message.SetError(&models.TransformationError{
				SafeMessage: err.Error(),
				Err:         err,
			})
			return nil, nil, message, nil
		}

		if err, ok := jqOutput.(error); ok {
			message.SetError(&models.TransformationError{
				SafeMessage: "jq output is an error",
				Err:         err,
			})
			return nil, nil, message, nil
		}

		return jqOutputHandler(jqOutput)(message, parsedEvent)
	}
}

// mkJQInput ensures the input to JQ query is of expected type
func mkJQInput(message *models.Message, interState any, spMode bool) (map[string]any, analytics.ParsedEvent, error) {
	if !spMode {
		// gojq input can only be map[string]any or []any
		// here we only consider the first, but we could also expand
		var input map[string]any
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
