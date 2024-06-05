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

package models

import "time"

// MessageBatch houses batches of messages, for batch transformations to operate across
type MessageBatch struct {
	OriginalMessages    []*Message        // Most targets will use the data from here, but where we have a http templating transformation, we would use this to ack batches of messages
	BatchData           []byte            // Where we template http requests, we use this to define the body of the request
	HTTPHeaders         map[string]string // For dynamic headers feature
	TimeRequestStarted  time.Time
	TimeRequestFinished time.Time
}

// BatchTransformationResult houses the result of a batch transformation
type BatchTransformationResult struct {
	Success   []MessageBatch
	Invalid   []*Message
	Oversized []*Message
}
