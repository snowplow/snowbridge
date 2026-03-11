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

package models

// TargetWriteResult contains the results from a target write operation
type TargetWriteResult struct {
	// Sent holds all the messages that were successfully sent to the target
	// and therefore have been acked by the target successfully.
	Sent []*Message

	// Failed contains all the messages that could not be sent to the target
	// and that should be retried.
	Failed []*Message

	// Invalid contains all the messages that cannot be sent to the target
	// due to various parseability reasons.  These messages cannot be retried
	// and need to be specially handled.
	Invalid []*Message
}

// NewTargetWriteResult builds a result structure to return from a target write attempt.
func NewTargetWriteResult(sent []*Message, failed []*Message, invalid []*Message) *TargetWriteResult {
	return &TargetWriteResult{
		Sent:    sent,
		Failed:  failed,
		Invalid: invalid,
	}
}
