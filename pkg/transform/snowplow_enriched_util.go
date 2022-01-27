// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

func intermediateAsSpEnrichedParsed(intermediateState interface{}, message *models.Message) (analytics.ParsedEvent, error) {
	var parsedMessage, ok = intermediateState.(analytics.ParsedEvent)
	var parseErr error
	if ok {
		return parsedMessage, nil
	}
	parsedMessage, parseErr = analytics.ParseEvent(string(message.Data))
	if parseErr != nil {
		return nil, parseErr
	}
	return parsedMessage, nil
}
