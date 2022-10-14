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
