// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package failure

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/target/targetiface"
	"github.com/snowplow-devops/stream-replicator/third_party/snowplow/badrows"
)

// SnowplowFailure holds a new client for transforming failed messages and emitting
// them to a target
type SnowplowFailure struct {
	processorArtifact string
	processorVersion  string
	target            targetiface.Target
	log               *log.Entry
}

// NewSnowplowFailure will create a new client for handling failed events
// by converting them into Snowplow compatible bad events and pushing them to
// a stream
func NewSnowplowFailure(target targetiface.Target, processorArtifact string, processorVersion string) (*SnowplowFailure, error) {
	return &SnowplowFailure{
		processorArtifact: processorArtifact,
		processorVersion:  processorVersion,
		target:            target,
		log:               log.WithFields(log.Fields{"failed": "snowplow"}),
	}, nil
}

// WriteInvalid will handle the conversion of invalid messages into failure
// messages that will then pushed to the specified target
func (d *SnowplowFailure) WriteInvalid(invalid []*models.Message) (*models.TargetWriteResult, error) {
	var transformed []*models.Message

	for _, msg := range invalid {
		var failureErrors []string

		err := msg.GetError()
		if err != nil {
			failureErrors = append(failureErrors, err.Error())
		}

		sv, err := badrows.NewGenericError(
			&badrows.GenericErrorInput{
				ProcessorArtifact: d.processorArtifact,
				ProcessorVersion:  d.processorVersion,
				Payload:           msg.Data,
				FailureTimestamp:  msg.TimePulled,
				FailureErrors:     failureErrors,
			},
			d.target.MaximumAllowedMessageSizeBytes(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to transform invalid message to snowplow.generic_error bad-row JSON")
		}

		svCompact, err := sv.Compact()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get compacted snowplow.generic_error bad-row JSON")
		}

		tMsg := msg
		tMsg.Data = []byte(svCompact)

		transformed = append(transformed, tMsg)
	}

	return d.target.Write(transformed)
}

// WriteOversized will handle the conversion of oversized messages into failure
// messages that will then pushed to the specified target
func (d *SnowplowFailure) WriteOversized(maximumAllowedSizeBytes int, oversized []*models.Message) (*models.TargetWriteResult, error) {
	var transformed []*models.Message

	for _, msg := range oversized {
		sv, err := badrows.NewSizeViolation(
			&badrows.SizeViolationInput{
				ProcessorArtifact:              d.processorArtifact,
				ProcessorVersion:               d.processorVersion,
				Payload:                        msg.Data,
				FailureTimestamp:               msg.TimePulled,
				FailureMaximumAllowedSizeBytes: maximumAllowedSizeBytes,
				FailureExpectation:             "Expected payload to fit into requested target",
			},
			d.target.MaximumAllowedMessageSizeBytes(),
		)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to transform oversized message to snowplow.size_violation bad-row JSON")
		}

		svCompact, err := sv.Compact()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to get compacted snowplow.size_violation bad-row JSON")
		}

		tMsg := msg
		tMsg.Data = []byte(svCompact)

		transformed = append(transformed, tMsg)
	}

	return d.target.Write(transformed)
}

// Open manages opening the underlying target
func (d *SnowplowFailure) Open() {
	d.target.Open()
}

// Close manages closing the underlying target
func (d *SnowplowFailure) Close() {
	d.target.Close()
}

// GetID returns the identifier for this target
func (d *SnowplowFailure) GetID() string {
	return d.target.GetID()
}
