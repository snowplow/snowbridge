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

package decompression

import (
	"testing"

	"github.com/snowplow/snowbridge/v5/pkg/testutil"
)

// decompress runs Decompress to completion and returns goods and bads in
// the order they were yielded.
func decompress(cfg Config, input []byte) ([]DecompressedRecord, []BadRecord) {
	var goods []DecompressedRecord
	var bads []BadRecord
	for res := range Decompress(cfg, input) {
		switch res.Kind {
		case ResultRecord:
			goods = append(goods, res.Record)
		case ResultBad:
			bads = append(bads, res.Bad)
		}
	}
	return goods, bads
}

// snowplowWrapper is the structural representation of a decompressed
// Snowplow stream-record body: a 2-byte header followed by zero or more
// length-prefixed inner records.
type snowplowWrapper struct {
	FormatVersion  byte
	PayloadVersion byte
	Records        [][]byte
}

// zstdCompress serializes the wrapper and returns its ZSTD-compressed form.
func zstdCompress(t *testing.T, wrapper snowplowWrapper) []byte {
	t.Helper()
	return testutil.ZstdCompressPayload(t, wrapper.FormatVersion, wrapper.PayloadVersion, wrapper.Records)
}

// zstdCompressBytes ZSTD-compresses an arbitrary byte slice. Used by tests
// that inject malformed inner content the wrapper can't express.
func zstdCompressBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	return testutil.ZstdCompressRawBytes(t, raw)
}

// gzipCompress serializes the wrapper and returns its gzip-compressed form.
func gzipCompress(t *testing.T, wrapper snowplowWrapper) []byte {
	t.Helper()
	return testutil.GzipCompressPayload(t, wrapper.FormatVersion, wrapper.PayloadVersion, wrapper.Records)
}

// gzipCompressBytes gzip-compresses an arbitrary byte slice. Used by tests
// that inject malformed inner content the wrapper can't express.
func gzipCompressBytes(t *testing.T, raw []byte) []byte {
	t.Helper()
	return testutil.GzipCompressRawBytes(t, raw)
}

// testCodec abstracts over a compression algorithm for table-driven tests.
type testCodec struct {
	name        string
	compress    func(t *testing.T, w snowplowWrapper) []byte
	compressRaw func(t *testing.T, raw []byte) []byte
}

// allCodecs lists the compression algorithms exercised by shared tests.
var allCodecs = []testCodec{
	{name: "zstd", compress: zstdCompress, compressRaw: zstdCompressBytes},
	{name: "gzip", compress: gzipCompress, compressRaw: gzipCompressBytes},
}
