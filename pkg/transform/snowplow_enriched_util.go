//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package transform

import (
	"github.com/pkg/errors"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"

	"github.com/snowplow/snowbridge/pkg/models"
)

// IntermediateAsSpEnrichedParsed returns the intermediate state as a ParsedEvent if valid or parses
// the message as an event
func IntermediateAsSpEnrichedParsed(intermediateState interface{}, message *models.Message) (analytics.ParsedEvent, error) {
	var parsedEvent, ok = intermediateState.(analytics.ParsedEvent)
	var parseErr error
	if ok {
		return parsedEvent, nil
	}
	parsedEvent, parseErr = analytics.ParseEvent(string(message.Data))
	if parseErr != nil {
		return nil, parseErr
	}
	return parsedEvent, nil
}

// ValidateAtomicField is a helper function to allow us to fail invalid atomic fields on startup
func ValidateAtomicField(field string) error {
	parsedEvent, parseErr := analytics.ParseEvent(string(SnowplowTsv1))
	if parseErr != nil {
		return parseErr
	}

	_, err := parsedEvent.GetValue(field)
	// if our test data is empty for the field in question, we'll get an EmptyFieldErr.
	if err != nil && err.Error() == analytics.EmptyFieldErr {
		return nil
	}

	return errors.Wrap(err, "error validating atomic field")
}
