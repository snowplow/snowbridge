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

const SnowplowFailureTarget = "snowplow"

// SnowplowFailure holds a client for transforming failed messages into failure payloads
type SnowplowFailure struct {
	processorArtifact           string
	processorVersion            string
	failureTargetMaxMessageSize int
	log                         *log.Entry
}

// NewSnowplowFailure will create a new client for handling failed events
// by converting them into Snowplow compatible bad events
func NewSnowplowFailure(failureTargetMaxMessageSize int, processorArtifact string, processorVersion string) (*SnowplowFailure, error) {
	return &SnowplowFailure{
		processorArtifact:           processorArtifact,
		processorVersion:            processorVersion,
		failureTargetMaxMessageSize: failureTargetMaxMessageSize,
		log:                         log.WithFields(log.Fields{"failed": SnowplowFailureTarget}),
	}, nil
}

// MakeInvalidPayloads transforms invalid messages into Snowplow generic_error bad-row format
func (d *SnowplowFailure) MakeInvalidPayloads(messages []*models.Message) ([]*models.Message, error) {
	var transformed []*models.Message

	for _, msg := range messages {
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
			d.failureTargetMaxMessageSize,
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

	return transformed, nil
}

// MakeOversizedPayloads transforms oversized messages into Snowplow size_violation bad-row format
func (d *SnowplowFailure) MakeOversizedPayloads(maximumAllowedSizeBytes int, messages []*models.Message) ([]*models.Message, error) {
	var transformed []*models.Message

	for _, msg := range messages {
		sv, err := badrows.NewSizeViolation(
			&badrows.SizeViolationInput{
				ProcessorArtifact:              d.processorArtifact,
				ProcessorVersion:               d.processorVersion,
				Payload:                        msg.Data,
				FailureTimestamp:               msg.TimePulled,
				FailureMaximumAllowedSizeBytes: maximumAllowedSizeBytes,
				FailureExpectation:             "Expected payload to fit into requested target",
			},
			d.failureTargetMaxMessageSize,
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

	return transformed, nil
}
