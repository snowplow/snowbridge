// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package badrows

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/snowplow-devops/stream-replicator/third_party/snowplow/iglu"
)

const (
	// ---------- [ BYTE COUNTERS ] ----------

	badRowWrapperBytes = 21 // {"schema":"","data":}

	// ---------- [ DATA KEYS ] ----------

	dataKeyProcessor = "processor"
	dataKeyFailure   = "failure"
	dataKeyPayload   = "payload"
)

// BadRow is the base structure for the data contained within a bad-row
type BadRow struct {
	schema             string
	selfDescribingData *iglu.SelfDescribingData
}

// NewBadRow returns a new bad-row structure
func NewBadRow(schema string, data map[string]interface{}, payload []byte, targetByteLimit int) (*BadRow, error) {
	payloadLength := len(payload)

	// Ensure data map does not contain anything for payload
	data[dataKeyPayload] = map[string]interface{}{}

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
