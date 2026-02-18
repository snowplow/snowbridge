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

package failure

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/third_party/snowplow/badrows"
)

const EventForwardingFailureTarget = "event_forwarding"

// EventForwardingFailure holds a client for transforming failed messages into failure payloads
type EventForwardingFailure struct {
	processorArtifact           string
	processorVersion            string
	failureTargetMessageMaxSize int
	log                         *log.Entry
}

// NewEventForwardingFailure will create a new client for handling failed events
// by converting them into EventForwarding compatible bad events
func NewEventForwardingFailure(failureTargetMessageMaxSize int, processorArtifact string, processorVersion string) (*EventForwardingFailure, error) {
	return &EventForwardingFailure{
		processorArtifact:           processorArtifact,
		processorVersion:            processorVersion,
		failureTargetMessageMaxSize: failureTargetMessageMaxSize,
		log:                         log.WithFields(log.Fields{"failed": EventForwardingFailureTarget}),
	}, nil
}

// MakeInvalidPayloads transforms invalid messages into Event Forwarding error format
func (ef *EventForwardingFailure) MakeInvalidPayloads(messages []*models.Message) ([]*models.Message, error) {
	var transformed []*models.Message

	for _, msg := range messages {
		err := msg.GetError()

		var sv *badrows.BadRow
		reportableError, ok := err.(models.SanitisedErrorMetadata)
		if ok {
			sv, err = badrows.NewEventForwardingError(
				&badrows.EventForwardingErrorInput{
					ProcessorArtifact: ef.processorArtifact,
					ProcessorVersion:  ef.processorVersion,
					OriginalTSV:       msg.OriginalData,
					ErrorType:         reportableError.Type(),
					LatestState:       msg.Data,
					ErrorMessage:      err.Error(),
					ErrorCode:         reportableError.Code(),
					FailureTimestamp:  msg.TimePulled,
				},
				ef.failureTargetMessageMaxSize,
			)
		} else {
			sv, err = badrows.NewEventForwardingError(
				&badrows.EventForwardingErrorInput{
					ProcessorArtifact: ef.processorArtifact,
					ProcessorVersion:  ef.processorVersion,
					OriginalTSV:       msg.OriginalData,
					LatestState:       msg.Data,
					ErrorMessage:      err.Error(),
					FailureTimestamp:  msg.TimePulled,
				},
				ef.failureTargetMessageMaxSize,
			)
		}

		if err != nil {
			return nil, errors.Wrap(err, "Failed to transform invalid message to event_forwarding.event_forwarding_error bad-row JSON")
		}

		svCompact, err := sv.Compact()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get compacted event_forwarding.event_forwarding_error bad-row JSON")
		}

		tMsg := msg
		tMsg.Data = []byte(svCompact)
		transformed = append(transformed, tMsg)
	}

	return transformed, nil
}

// MakeOversizedPayloads transforms oversized messages into Event Forwarding size_violation format
func (ef *EventForwardingFailure) MakeOversizedPayloads(maximumAllowedSizeBytes int, messages []*models.Message) ([]*models.Message, error) {
	var transformed []*models.Message

	for _, msg := range messages {
		sv, err := badrows.NewSizeViolation(
			&badrows.SizeViolationInput{
				ProcessorArtifact:              ef.processorArtifact,
				ProcessorVersion:               ef.processorVersion,
				Payload:                        msg.Data,
				FailureTimestamp:               msg.TimePulled,
				FailureMaximumAllowedSizeBytes: maximumAllowedSizeBytes,
				FailureExpectation:             "Expected payload to fit into requested target",
			},
			ef.failureTargetMessageMaxSize,
		)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to transform oversized message to event_forwarding.size_violation bad-row JSON")
		}

		svCompact, err := sv.Compact()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get compacted event_forwarding.size_violation bad-row JSON")
		}

		tMsg := msg
		tMsg.Data = []byte(svCompact)
		transformed = append(transformed, tMsg)
	}

	return transformed, nil
}
