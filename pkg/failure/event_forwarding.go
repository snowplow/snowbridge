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
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/target/targetiface"
	"github.com/snowplow/snowbridge/third_party/snowplow/badrows"
)

const EventForwardingFailureTarget = "event_forwarding"

// EventForwardingFailure holds a new client for transforming failed messages and emitting
// them to a target
type EventForwardingFailure struct {
	processorArtifact string
	processorVersion  string
	target            targetiface.Target
	log               *log.Entry
}

// NewEventForwardingFailure will create a new client for handling failed events
// by converting them into EventForwarding compatible bad events and pushing them to
// a stream
func NewEventForwardingFailure(target targetiface.Target, processorArtifact string, processorVersion string) (*EventForwardingFailure, error) {
	return &EventForwardingFailure{
		processorArtifact: processorArtifact,
		processorVersion:  processorVersion,
		target:            target,
		log:               log.WithFields(log.Fields{"failed": EventForwardingFailureTarget}),
	}, nil
}

// WriteInvalid will handle the conversion of invalid messages into failure
// messages that will then pushed to the specified target
func (ef *EventForwardingFailure) WriteInvalid(invalid []*models.Message) (*models.TargetWriteResult, error) {
	var transformed []*models.Message

	for _, msg := range invalid {
		var failureErrors []string

		err := msg.GetError()
		if err != nil {
			failureErrors = append(failureErrors, err.Error())
		}

		var sv *badrows.BadRow
		reportableError, ok := err.(models.ErrorMetadata)
		if ok {
			sv, err = badrows.NewEventForwardingError(
				&badrows.EventForwardingErrorInput{
					ProcessorArtifact: ef.processorArtifact,
					ProcessorVersion:  ef.processorVersion,
					OriginalTSV:       msg.OriginalData,
					ErrorType:         reportableError.ReportableType(),
					LatestState:       msg.Data,
					ErrorMessage:      reportableError.ReportableDescription(),
					ErrorCode:         reportableError.ReportableCode(),
					FailureTimestamp:  msg.TimePulled,
				},
				ef.target.MaximumAllowedMessageSizeBytes(),
			)
		} else {
			sv, err = badrows.NewEventForwardingError(
				&badrows.EventForwardingErrorInput{
					ProcessorArtifact: ef.processorArtifact,
					ProcessorVersion:  ef.processorVersion,
					OriginalTSV:       msg.OriginalData,
					LatestState:       msg.Data,
					ErrorMessage:      strings.Join(failureErrors, ": "),
					FailureTimestamp:  msg.TimePulled,
				},
				ef.target.MaximumAllowedMessageSizeBytes(),
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

	return ef.target.Write(transformed)
}

// WriteOversized will handle the conversion of oversized messages into failure
// messages that will then pushed to the specified target
func (ef *EventForwardingFailure) WriteOversized(maximumAllowedSizeBytes int, oversized []*models.Message) (*models.TargetWriteResult, error) {
	var transformed []*models.Message

	for _, msg := range oversized {
		sv, err := badrows.NewSizeViolation(
			&badrows.SizeViolationInput{
				ProcessorArtifact:              ef.processorArtifact,
				ProcessorVersion:               ef.processorVersion,
				Payload:                        msg.Data,
				FailureTimestamp:               msg.TimePulled,
				FailureMaximumAllowedSizeBytes: maximumAllowedSizeBytes,
				FailureExpectation:             "Expected payload to fit into requested target",
			},
			ef.target.MaximumAllowedMessageSizeBytes(),
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

	return ef.target.Write(transformed)
}

// Open manages opening the underlying target
func (ef *EventForwardingFailure) Open() {
	ef.target.Open()
}

// Close manages closing the underlying target
func (ef *EventForwardingFailure) Close() {
	ef.target.Close()
}

// GetID returns the identifier for this target
func (ef *EventForwardingFailure) GetID() string {
	return ef.target.GetID()
}
