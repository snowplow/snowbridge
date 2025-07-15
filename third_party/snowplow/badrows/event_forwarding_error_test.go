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

package badrows

import (
	"fmt"
	"testing"
	"time"

	"github.com/snowplow/snowbridge/pkg/testutil"

	"github.com/stretchr/testify/assert"
)

func TestNewEventForwardingError(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewEventForwardingError(
		&EventForwardingErrorInput{
			ProcessorArtifact: "snowbridge",
			ProcessorVersion:  "0.1.0",
			OriginalTSV:       []byte("\u0001"),
			ErrorType:         "transformation",
			LatestState:       []byte("\u0001"),
			ErrorMessage:      "",
			ErrorCode:         "",
			FailureTimestamp:  timeNow,
		},
		262144,
	)
	assert.Nil(err)
	assert.NotNil(sv)

	compact, err := sv.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	diff, err := testutil.GetJsonDiff(fmt.Sprintf(`{"data":{"errorCode":"","errorMessage":"","errorType":"transformation","latestState":"\u0001","originalTSV":"\u0001","payload":"\u0001","processor":{"artifact":"snowbridge","version":"0.1.0"},"timestamp":"%s"},"schema":"iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0"}`, timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
	assert.Nil(err)
	assert.Zero(diff)
}

func TestNewEventForwardingError_WithErrors(t *testing.T) {
	assert := assert.New(t)

	timeNow := time.Now()

	sv, err := NewEventForwardingError(
		&EventForwardingErrorInput{
			ProcessorArtifact: "snowbridge",
			ProcessorVersion:  "0.1.0",
			OriginalTSV:       []byte("\u0001"),
			ErrorType:         "api",
			LatestState:       []byte("\u0001"),
			ErrorMessage:      "Unauthorised",
			ErrorCode:         "401",
			FailureTimestamp:  timeNow,
		},
		262144,
	)
	assert.Nil(err)
	assert.NotNil(sv)

	compact, err := sv.Compact()
	assert.Nil(err)
	assert.NotNil(compact)

	diff, err := testutil.GetJsonDiff(fmt.Sprintf(`{"data":{"errorCode":"401","errorMessage":"Unauthorised","errorType":"api","latestState":"\u0001","originalTSV":"\u0001","payload":"\u0001","processor":{"artifact":"snowbridge","version":"0.1.0"},"timestamp":"%s"},"schema":"iglu:com.snowplowanalytics.snowplow.badrows/event_forwarding_error/jsonschema/1-0-0"}`, timeNow.UTC().Format("2006-01-02T15:04:05Z07:00")), compact)
	assert.Nil(err)
	assert.Zero(diff)
}
