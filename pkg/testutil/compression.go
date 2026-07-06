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

package testutil

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// BuildSnowplowInner encodes the Snowplow stream-record wire format:
// [format_version][payload_version] followed by zero or more [4-byte BE size][record bytes].
func BuildSnowplowInner(formatVersion, payloadVersion byte, records [][]byte) []byte {
	var b bytes.Buffer
	b.WriteByte(formatVersion)
	b.WriteByte(payloadVersion)
	for _, r := range records {
		var sz [4]byte
		binary.BigEndian.PutUint32(sz[:], uint32(len(r)))
		b.Write(sz[:])
		b.Write(r)
	}
	return b.Bytes()
}

// ZstdCompressRawBytes zstd-compresses an arbitrary byte slice.
// Used by tests that need to inject malformed Snowplow content into a valid zstd frame.
func ZstdCompressRawBytes(t testing.TB, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write(raw)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// ZstdCompressPayload builds a valid Snowplow zstd-compressed payload with the given records.
func ZstdCompressPayload(t testing.TB, formatVersion, payloadVersion byte, records [][]byte) []byte {
	t.Helper()
	return ZstdCompressRawBytes(t, BuildSnowplowInner(formatVersion, payloadVersion, records))
}

// GzipCompressRawBytes gzip-compresses an arbitrary byte slice.
// Used by tests that need to inject malformed Snowplow content into a valid gzip frame.
func GzipCompressRawBytes(t testing.TB, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(raw)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// GzipCompressPayload builds a valid Snowplow gzip-compressed payload with the given records.
func GzipCompressPayload(t testing.TB, formatVersion, payloadVersion byte, records [][]byte) []byte {
	t.Helper()
	return GzipCompressRawBytes(t, BuildSnowplowInner(formatVersion, payloadVersion, records))
}
