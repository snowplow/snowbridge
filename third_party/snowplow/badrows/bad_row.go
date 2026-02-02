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
	"unicode/utf8"

	"github.com/pkg/errors"

	"github.com/snowplow/snowbridge/v3/third_party/snowplow/iglu"
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

// newBadRow handles oversized payloads and returns a new bad-row structure
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
		// We need to use utf8 rune to correctly trim not standard characters which may be in the input,
		// ie Japanese characters.
		truncateAt := bytesForPayload
		for truncateAt > 0 && !utf8.RuneStart(payload[truncateAt]) {
			truncateAt--
		}
		data[dataKeyPayload] = string(payload[:truncateAt])
	} else {
		data[dataKeyPayload] = string(payload)
	}

	// Verify that the final JSON output fits within the target byte limit
	// If not, iteratively reduce payload size until it fits
	for {
		testBadRow := &BadRow{
			schema:             schema,
			selfDescribingData: iglu.NewSelfDescribingData(schema, data),
		}

		compact, err := testBadRow.Compact()
		if err != nil {
			return nil, errors.Wrap(err, "Could not compact bad-row for size validation")
		}

		// If it fits within the limit, we're done
		if len(compact) <= targetByteLimit {
			return testBadRow, nil
		}

		// If JSON is oversized, we need to reduce the payload by further 10%
		currentPayload := data[dataKeyPayload].(string)
		newLength := len(currentPayload) * 9 / 10

		// Truncate at UTF-8 boundary
		payloadBytes := []byte(currentPayload)
		truncateAt := newLength
		for truncateAt > 0 && !utf8.RuneStart(payloadBytes[truncateAt]) {
			truncateAt--
		}

		data[dataKeyPayload] = string(payloadBytes[:truncateAt])
	}
}

// newBadRowEventForwardingError handles oversized payloads and latestState using JSON-aware truncation
func newBadRowEventForwardingError(schema string, data map[string]any, payload []byte, latestState []byte, targetByteLimit int) (*BadRow, error) {
	// Ensure data map does not contain anything for payload or latest state
	data[dataKeyPayload] = ""

	// Ensure data map contains failure data
	if data[dataKeyFailure] == nil {
		return nil, errors.New("Error creating bad data - failure data is nil")
	}
	failureMap, ok := data[dataKeyFailure].(map[string]string)
	if !ok {
		return nil, errors.New("Error creating bad data - failure data is not a map[string]string")
	}

	// Add latestState and payload to the data
	failureMap[dataKeyLatestState] = string(latestState)
	data[dataKeyFailure] = failureMap
	data[dataKeyPayload] = string(payload)

	// Use JSON-aware iterative truncation to ensure final result fits within target limit
	for {
		testBadRow := &BadRow{
			schema:             schema,
			selfDescribingData: iglu.NewSelfDescribingData(schema, data),
		}

		compact, err := testBadRow.Compact()
		if err != nil {
			return nil, errors.Wrap(err, "Could not compact bad-row for size validation")
		}

		// If it fits within the limit, we're done
		if len(compact) <= targetByteLimit {
			return testBadRow, nil
		}

		// If JSON is oversized, prioritize truncating latestState first, then payload
		currentLatestState := failureMap[dataKeyLatestState]
		currentPayload := data[dataKeyPayload].(string)

		if len(currentLatestState) > 0 {
			// Reduce latestState size by 10%
			newLength := len(currentLatestState) * 9 / 10

			// Truncate at UTF-8 boundary
			latestStateBytes := []byte(currentLatestState)
			truncateAt := newLength
			for truncateAt > 0 && !utf8.RuneStart(latestStateBytes[truncateAt]) {
				truncateAt--
			}

			failureMap[dataKeyLatestState] = string(latestStateBytes[:truncateAt])
			data[dataKeyFailure] = failureMap
		} else if len(currentPayload) > 0 {
			// If latestState is empty, reduce payload size by 10%
			newLength := len(currentPayload) * 9 / 10

			// Truncate at UTF-8 boundary
			payloadBytes := []byte(currentPayload)
			truncateAt := newLength
			for truncateAt > 0 && !utf8.RuneStart(payloadBytes[truncateAt]) {
				truncateAt--
			}

			data[dataKeyPayload] = string(payloadBytes[:truncateAt])
		} else {
			// Both payload and latestState are empty but still doesn't fit
			return nil, errors.New("Failed to create bad-row as resultant payload will exceed the targets byte limit")
		}
	}
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
