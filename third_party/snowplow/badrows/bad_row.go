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
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/snowplow/snowbridge/third_party/snowplow/iglu"
)

const (
	// ---------- [ BYTE COUNTERS ] ----------

	badRowWrapperBytes = 21 // {"schema":"","data":}

	// ---------- [ DATA KEYS ] ----------

	dataKeyProcessor = "processor"
	dataKeyFailure   = "failure"
	dataKeyPayload   = "payload"

	dataKeyErrorType    = "errorType"
	dataKeyLatestState  = "latestState"
	dataKeyErrorMessage = "errorMessage"
	dataKeyErrorCode    = "errorCode"
	dataKeyTimestamp    = "timestamp"
)

// BadRow is the base structure for the data contained within a bad-row
type BadRow struct {
	schema             string
	selfDescribingData *iglu.SelfDescribingData
}

// newBadRow returns a new bad-row structure
func newBadRow(schema string, data map[string]any, payload []byte, targetByteLimit int) (*BadRow, error) {
	payloadLength := len(payload)

	// Ensure data map does not contain anything for payload
	data[dataKeyPayload] = ""

	// Check bytes allocated to data map (without payload)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "Could not unmarshall bad-row data blob to JSON")
	}
	currentByteCount := len(schema) + badRowWrapperBytes + len(dataBytes)

	// Figure out if we have enough bytes left to include the payload (or a truncated payload)
	bytesForPayload := targetByteLimit - currentByteCount
	if bytesForPayload <= 0 {
		return nil, errors.New("Failed to create bad-row as resultant payload will exceed the targets byte limit")
	}

	// Add the payload into the data map
	if payloadLength > bytesForPayload {
		data[dataKeyPayload] = string(payload[:bytesForPayload])
	} else {
		data[dataKeyPayload] = string(payload)
	}

	return &BadRow{
		schema: schema,
		selfDescribingData: iglu.NewSelfDescribingData(
			schema,
			data,
		),
	}, nil
}

// newBadRowEventForwardingError does the same thing, but allows for the more complex payloads in this bad row type
func newBadRowEventForwardingError(schema string, data map[string]any, payload []byte, latestState []byte, targetByteLimit int) (*BadRow, error) {

	latestStateLength := len(latestState)

	// Ensure data map does not contain anything for payload or latest state
	data[dataKeyPayload] = ""
	failureMap := data[dataKeyFailure].(map[string]string)
	failureMap[dataKeyLatestState] = ""
	data[dataKeyFailure] = failureMap

	// Check bytes allocated to data map (without payload)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "Could not unmarshall bad-row data blob to JSON")
	}

	currentByteCount := len(schema) + badRowWrapperBytes + len(dataBytes)

	// Figure out if we have enough bytes left to include the latestState (or a truncated latestState)
	// We include the length of the payload in this calculation, because we'd rather truncate the latestState if one of them needs it.
	bytesForLatestState := targetByteLimit - currentByteCount - len(payload)
	if bytesForLatestState <= 0 {
		// Unlike in newBadRow, we might have enough room for a payload or truncated payload in this case.
		// So we'll allocate 0 bytes to latestState and proceed with the payload.
		bytesForLatestState = 0
	}

	// First provide latestState
	if latestStateLength > bytesForLatestState {
		failureMap[dataKeyLatestState] = string(latestState[:bytesForLatestState])
	} else {
		failureMap[dataKeyLatestState] = string(latestState)
	}

	data[dataKeyFailure] = failureMap

	// Now we can let the previous function handle the rest
	return newBadRow(
		schema,
		data,
		payload,
		targetByteLimit)
}

// Compact returns a compacted version of this badrow
func (br *BadRow) Compact() (string, error) {
	return br.selfDescribingData.String()
}

// --- Helpers

// formatTimeISO8601 will format the time as ISO 8601
func formatTimeISO8601(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z07:00")
}
