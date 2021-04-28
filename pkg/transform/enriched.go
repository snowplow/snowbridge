// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// EnrichedToJson is a specific transformation implementation to transform good enriched data within a message to Json
func EnrichedToJson(messages []*models.Message) ([]*models.Message, []*models.Message) {
	successes := make([]*models.Message, 0, len(messages))
	failures := make([]*models.Message, 0, len(messages))

	for _, message := range messages {
		parsedMessage, err := analytics.ParseEvent(string(message.Data))
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
			continue
		}
		jsonMessage, err := parsedMessage.ToJson()
		if err != nil {
			message.SetError(err)
			failures = append(failures, message)
			continue
		}
		message.Data = jsonMessage // because we're using a pointer, this alters the original value I think. Is this is acceptable?
		successes = append(successes, message)

	}
	return successes, failures // Doesn't return any err as errors should all go into failures.
}
