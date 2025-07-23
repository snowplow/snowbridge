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
	dataKeyOriginalTSV  = "originalTSV"
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

// TODO: We need to find a pattern that allows us to handle oversized payloads where it's an object as well
// Note that this is used for ovesized as well as generic errors - that's a factor here.
func newBadRowEventForwardingError(schema string, data map[string]any, payload map[string]string, targetByteLimit int) (*BadRow, error) {

	originalTSV := []byte(payload[dataKeyOriginalTSV])
	latestState := []byte(payload[dataKeyLatestState])

	originalTSVLength := len(originalTSV)
	latestStateLength := len(latestState)

	payloadMap := map[string]string{
		dataKeyOriginalTSV: "",
		dataKeyLatestState: "",
	}

	// Ensure data map does not contain anything for payload
	data[dataKeyPayload] = payloadMap

	// Check bytes allocated to data map (without payload)
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrap(err, "Could not unmarshall bad-row data blob to JSON")
	}

	currentByteCount := len(schema) + badRowWrapperBytes + len(dataBytes)

	// Figure out if we have enough bytes left to include the payload (or a truncated payload)
	bytesForOriginalTSV := targetByteLimit - currentByteCount
	if bytesForOriginalTSV <= 0 {
		return nil, errors.New("Failed to create bad-row as resultant payload will exceed the targets byte limit")
	}

	var bytesForLatestState int

	// First provide original data
	if originalTSVLength > bytesForOriginalTSV {
		payloadMap[dataKeyOriginalTSV] = string(originalTSV[:bytesForOriginalTSV])
		// We had to trim original, so no room for latest
		bytesForLatestState = 0
	} else {
		payloadMap[dataKeyOriginalTSV] = string(originalTSV)
		// Can't be negative since we're in the else case
		bytesForLatestState = bytesForOriginalTSV - originalTSVLength
	}

	// Now latest state
	if latestStateLength > bytesForLatestState {
		payloadMap[dataKeyLatestState] = string(latestState[:bytesForLatestState])
	} else {
		payloadMap[dataKeyLatestState] = string(latestState)
	}

	data[dataKeyPayload] = payloadMap

	return &BadRow{
		schema: schema,
		selfDescribingData: iglu.NewSelfDescribingData(
			schema,
			data,
		),
	}, nil
}

// newBadRow returns a new bad-row structure
func newBadRow(schema string, data map[string]any, payload []byte, targetByteLimit int) (*BadRow, error) {
	payloadLength := len(payload)

	// Ensure data map does not contain anything for payload
	data[dataKeyPayload] = map[string]any{}

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

// Compact returns a compacted version of this badrow
func (br *BadRow) Compact() (string, error) {
	return br.selfDescribingData.String()
}

// --- Helpers

// formatTimeISO8601 will format the time as ISO 8601
func formatTimeISO8601(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z07:00")
}
