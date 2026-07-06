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
	"github.com/snowplow/snowbridge/v5/pkg/transform"
)

var benchDecompressCfg = Config{MaxBytesSinglePayload: 10 * 1024 * 1024}

type benchCase struct {
	name    string
	records [][]byte
}

func makeBenchCases() []benchCase {
	base := [][]byte{transform.SnowplowTsv1, transform.SnowplowTsv2, transform.SnowplowTsv3}
	batch100 := make([][]byte, 100)
	for i := range batch100 {
		batch100[i] = base[i%len(base)]
	}
	batch1000 := make([][]byte, 1000)
	for i := range batch1000 {
		batch1000[i] = base[i%len(base)]
	}
	return []benchCase{
		{name: "single", records: [][]byte{transform.SnowplowTsv1}},
		{name: "batch_3", records: base},
		{name: "batch_100", records: batch100},
		{name: "batch_1000", records: batch1000},
	}
}

func totalBytes(records [][]byte) int64 {
	var n int64
	for _, r := range records {
		n += int64(len(r))
	}
	return n
}

func BenchmarkDecompress_Zstd(b *testing.B) {
	for _, bc := range makeBenchCases() {
		b.Run(bc.name, func(b *testing.B) {
			input := testutil.ZstdCompressPayload(b, 1, 1, bc.records)
			b.SetBytes(totalBytes(bc.records))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for range Decompress(benchDecompressCfg, input) {
				}
			}
		})
	}
}

func BenchmarkDecompress_Gzip(b *testing.B) {
	for _, bc := range makeBenchCases() {
		b.Run(bc.name, func(b *testing.B) {
			input := testutil.GzipCompressPayload(b, 1, 1, bc.records)
			b.SetBytes(totalBytes(bc.records))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				for range Decompress(benchDecompressCfg, input) {
				}
			}
		})
	}
}
