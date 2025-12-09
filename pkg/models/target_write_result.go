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

	// Oversized holds all the messages that were too big to be sent to
	// the target and need to be handled externally to the target.
	Oversized []*Message

	// Invalid contains all the messages that cannot be sent to the target
	// due to various parseability reasons.  These messages cannot be retried
	// and need to be specially handled.
	Invalid []*Message
}

// NewTargetWriteResult builds a result structure to return from a target write attempt.
func NewTargetWriteResult(sent []*Message, failed []*Message, oversized []*Message, invalid []*Message) *TargetWriteResult {
	return &TargetWriteResult{
		Sent:      sent,
		Failed:    failed,
		Oversized: oversized,
		Invalid:   invalid,
	}
}

// Append will add another write result to the source one to allow for
// result concatenation and then return the resultant struct
func (wr *TargetWriteResult) Append(nwr *TargetWriteResult) *TargetWriteResult {
	wrC := *wr

	if nwr != nil {
		wrC.Sent = append(wrC.Sent, nwr.Sent...)
		wrC.Failed = append(wrC.Failed, nwr.Failed...)
		wrC.Oversized = append(wrC.Oversized, nwr.Oversized...)
		wrC.Invalid = append(wrC.Invalid, nwr.Invalid...)
	}

	return &wrC
}
