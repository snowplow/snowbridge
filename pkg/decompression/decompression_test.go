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
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecompress_EmptyInput_NoYield(t *testing.T) {
	goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, nil)
	assert.Empty(t, goods)
	assert.Empty(t, bads)

	goods, bads = decompress(Config{MaxBytesSinglePayload: 1000000}, []byte{})
	assert.Empty(t, goods)
	assert.Empty(t, bads)
}

func TestDecompress_Uncompressed(t *testing.T) {
	cases := map[string][]byte{
		"single byte":          {0x42},
		"two bytes (no magic)": {0xAB, 0xCD},
		"three bytes":          {0x01, 0x02, 0x03},
		"plain ascii payload":  []byte("hello, snowplow"),
		"binary-ish payload":   {0x00, 0x01, 0x02, 0x03, 0x04, 0xFF, 0xFE, 0xFD},
		"partial zstd magic":   {0x28, 0xB5, 0x2F},       // 3 of 4 zstd magic bytes
		"non-magic 4+ bytes":   {0xDE, 0xAD, 0xBE, 0xEF}, // not gzip, not zstd
		"starts with gzip 1st": {0x1F, 0x00, 0x00},       // only first gzip byte matches
		"starts with zstd 1st": {0x28, 0x00, 0x00, 0x00}, // only first zstd byte matches
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, input)
			require.Len(t, goods, 1)
			assert.Empty(t, bads)
			assert.Equal(t, input, goods[0].Data)
			assert.Nil(t, goods[0].PayloadVersion, "uncompressed records have no header, so PayloadVersion must be nil")
		})
	}
}

func TestDecompress_SingleRecord(t *testing.T) {
	rawRecord := []byte(`{"event":"hello"}`)
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  1,
				PayloadVersion: 7,
				Records:        [][]byte{rawRecord},
			})

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			require.Len(t, goods, 1)
			require.Empty(t, bads)
			assert.Equal(t, rawRecord, goods[0].Data)
			require.NotNil(t, goods[0].PayloadVersion)
			assert.Equal(t, 7, *goods[0].PayloadVersion)
		})
	}
}

func TestDecompress_ManyRecordsVaryingSizes(t *testing.T) {
	// Mix of tiny, medium, and large records, ~100 in total — covers both
	// the high-volume read loop and varying allocation sizes.
	rawRecords := make([][]byte, 0, 100)
	for i := range 100 {
		switch i % 4 {
		case 0:
			rawRecords = append(rawRecords, []byte("x"))
		case 1:
			rawRecords = append(rawRecords, []byte(fmt.Sprintf("record-%d", i)))
		case 2:
			rawRecords = append(rawRecords, bytes.Repeat([]byte("medium "), 20))
		case 3:
			rawRecords = append(rawRecords, bytes.Repeat([]byte{byte(i)}, 1024))
		}
	}

	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  1,
				PayloadVersion: 1,
				Records:        rawRecords,
			})

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			require.Empty(t, bads)
			require.Len(t, goods, len(rawRecords))

			for i, r := range rawRecords {
				assert.Equal(t, r, goods[i].Data)
				assert.Equal(t, 1, *goods[i].PayloadVersion)
			}
		})
	}
}

func TestDecompress_PayloadVersionPropagated(t *testing.T) {
	// Spec: the second header byte is opaque to the decompressor — it should
	// surface whatever value was written, including 0 and 255.
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			for _, v := range []byte{0, 1, 42, 255} {
				t.Run(fmt.Sprintf("v=%d", v), func(t *testing.T) {
					compressedInput := c.compress(t, snowplowWrapper{
						FormatVersion:  1,
						PayloadVersion: v,
						Records:        [][]byte{[]byte("x")},
					})
					goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
					require.Empty(t, bads)
					require.Len(t, goods, 1)
					require.NotNil(t, goods[0].PayloadVersion)
					assert.Equal(t, int(v), *goods[0].PayloadVersion)
				})
			}
		})
	}
}

func TestDecompress_HeaderOnlyNoRecords(t *testing.T) {
	// Valid header but zero records is well-formed — clean EOF after header.
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{FormatVersion: 1, PayloadVersion: 1})
			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			assert.Empty(t, goods)
			assert.Empty(t, bads)
		})
	}
}

func TestDecompress_ZeroByteRecord(t *testing.T) {
	// A 0-length record (size prefix = 0) is structurally valid.
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  1,
				PayloadVersion: 1,
				Records:        [][]byte{{}, []byte("after-empty")},
			})
			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			require.Empty(t, bads)
			require.Len(t, goods, 2)
			assert.Empty(t, goods[0].Data)
			assert.Equal(t, []byte("after-empty"), goods[1].Data)
		})
	}
}

func TestDecompress_UnsupportedFormatVersion(t *testing.T) {
	// First header byte is the compression-format version. Anything other
	// than 1 must surface as UnsupportedVersion (per spec step 3).
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  2,
				PayloadVersion: 1,
				Records:        [][]byte{[]byte("payload")},
			})

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			assert.Empty(t, goods)
			require.Len(t, bads, 1)
			assert.Equal(t, UnsupportedVersion, bads[0].Kind)
			assert.Equal(t, fmt.Sprintf("%s: unsupported versions in compressed record header: 2, 1", c.name), bads[0].Message)
		})
	}
}

func TestDecompress_ShortHeader(t *testing.T) {
	// Valid compressed frame containing only 1 of the 2 header bytes — the reader
	// gets a partial header and surfaces CorruptInput.
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compressRaw(t, []byte{0x01})
			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			assert.Empty(t, goods)
			require.Len(t, bads, 1)
			assert.Equal(t, CorruptInput, bads[0].Kind)
			assert.Equal(t, fmt.Sprintf("%s: short header", c.name), bads[0].Message)
			assert.Error(t, bads[0].Err)
		})
	}
}

func TestDecompress_TruncatedSizePrefix(t *testing.T) {
	// Header (2 bytes) plus an incomplete size prefix (1, 2, or 3 of 4 bytes).
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			for n := 1; n <= 3; n++ {
				t.Run(fmt.Sprintf("%d_of_4_bytes", n), func(t *testing.T) {
					body := append([]byte{1, 1}, bytes.Repeat([]byte{0x00}, n)...)
					compressedInput := c.compressRaw(t, body)

					goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
					assert.Empty(t, goods)
					require.Len(t, bads, 1)
					assert.Equal(t, CorruptInput, bads[0].Kind)
				})
			}
		})
	}
}

func TestDecompress_TruncatedRecord(t *testing.T) {
	// Size prefix declares 100 bytes but the stream only carries 10.
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			var inner bytes.Buffer
			inner.WriteByte(1)
			inner.WriteByte(1)
			var sz [4]byte
			binary.BigEndian.PutUint32(sz[:], 100)
			inner.Write(sz[:])
			inner.Write(bytes.Repeat([]byte{0xFF}, 10))

			compressedInput := c.compressRaw(t, inner.Bytes())

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			assert.Empty(t, goods)
			require.Len(t, bads, 1)
			assert.Equal(t, CorruptInput, bads[0].Kind)
		})
	}
}

func TestDecompress_NegativeSize(t *testing.T) {
	// A size with the high bit set decodes to a negative int32 — must be
	// rejected to guard against wraparound and malformed headers.
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			var inner bytes.Buffer
			inner.WriteByte(1)
			inner.WriteByte(1)
			var sz [4]byte
			binary.BigEndian.PutUint32(sz[:], 0x80000000)
			inner.Write(sz[:])

			compressedInput := c.compressRaw(t, inner.Bytes())

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			assert.Empty(t, goods)
			require.Len(t, bads, 1)
			assert.Equal(t, CorruptInput, bads[0].Kind)
		})
	}
}

func TestDecompress_SizeViolationKeepsStreamOpen(t *testing.T) {
	good1 := []byte("ok-small")
	tooBig := bytes.Repeat([]byte{0xCC}, 5000)
	good2 := []byte("also-small")

	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  1,
				PayloadVersion: 1,
				Records:        [][]byte{good1, tooBig, good2},
			})

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1024}, compressedInput)

			require.Len(t, goods, 2)
			assert.Equal(t, good1, goods[0].Data)
			assert.Equal(t, good2, goods[1].Data)

			require.Len(t, bads, 1)
			assert.Equal(t, SizeViolation, bads[0].Kind)
			assert.Equal(t, fmt.Sprintf("%s: record exceeds configured maximum size of 1024 bytes after decompression", c.name), bads[0].Message)
			assert.EqualError(t, bads[0].Err, "actual record size 5000 bytes")
		})
	}
}

func TestDecompress_TrailingBytesAfterFrame(t *testing.T) {
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			headerOnly := c.compress(t, snowplowWrapper{FormatVersion: 1, PayloadVersion: 1})

			trailing := append([]byte{0, 0, 0, 5}, bytes.Repeat([]byte{0}, 20)...)
			compressedInput := append(headerOnly, trailing...)

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			assert.Empty(t, goods)
			require.Len(t, bads, 1)
			assert.Equal(t, CorruptInput, bads[0].Kind)
		})
	}
}

// Verifies via Go's iter.Seq protocol: when the loop breaks, the next yield
// returns false. If the iterator function returned by Decompress ignored that
// and kept calling yield, the runtime would panic — so a clean exit with one
// record proves Decompress honours the stop signal.
func TestDecompress_StopsOnEarlyBreak(t *testing.T) {
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  1,
				PayloadVersion: 1,
				Records:        [][]byte{[]byte("first"), []byte("second"), []byte("third")},
			})

			var goods []DecompressedRecord
			for res := range Decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput) {
				goods = append(goods, res.Record)
				break
			}

			require.Len(t, goods, 1)
			assert.Equal(t, []byte("first"), goods[0].Data)
		})
	}
}

func TestDecompress_SizeAtBoundary(t *testing.T) {
	const limit = 100

	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			t.Run("at_limit_passes", func(t *testing.T) {
				rawRecord := bytes.Repeat([]byte{0xAA}, limit)
				compressedInput := c.compress(t, snowplowWrapper{
					FormatVersion:  1,
					PayloadVersion: 1,
					Records:        [][]byte{rawRecord},
				})

				goods, bads := decompress(Config{MaxBytesSinglePayload: limit}, compressedInput)
				require.Empty(t, bads)
				require.Len(t, goods, 1)
				assert.Equal(t, rawRecord, goods[0].Data)
			})

			t.Run("limit_plus_one_fails", func(t *testing.T) {
				rawRecord := bytes.Repeat([]byte{0xAA}, limit+1)
				compressedInput := c.compress(t, snowplowWrapper{
					FormatVersion:  1,
					PayloadVersion: 1,
					Records:        [][]byte{rawRecord},
				})

				goods, bads := decompress(Config{MaxBytesSinglePayload: limit}, compressedInput)
				assert.Empty(t, goods)
				require.Len(t, bads, 1)
				assert.Equal(t, SizeViolation, bads[0].Kind)
			})
		})
	}
}

func TestDecompress_Gzip_CorruptCRC32(t *testing.T) {
	// Build a valid gzip frame containing just the 2-byte Snowplow header
	compressedInput := gzipCompress(t, snowplowWrapper{FormatVersion: 1, PayloadVersion: 1})

	// Zero-ing out the CRC32 field in the gzip trailer ([CRC32 (4 bytes)][ISIZE (4 bytes)]).
	// Go's gzip.Reader validates the CRC32 when the stream ends; the mismatch is
	// detected when the record loop tries to read the next size prefix and surfaces as CorruptInput.
	crcOffset := len(compressedInput) - 8
	compressedInput[crcOffset] = 0
	compressedInput[crcOffset+1] = 0
	compressedInput[crcOffset+2] = 0
	compressedInput[crcOffset+3] = 0

	goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
	assert.Empty(t, goods)
	require.Len(t, bads, 1)
	assert.Equal(t, CorruptInput, bads[0].Kind)
}

func TestDecompress_NonUTF8RecordContent(t *testing.T) {
	rawRecord := []byte{0x80, 0x01, 0x00, 0x02, 0xC0, 0xFF, 0xEE, 0xFE, 0xFD, 0xFC, 0x00, 0x7F, 0xFF}
	for _, c := range allCodecs {
		t.Run(c.name, func(t *testing.T) {
			compressedInput := c.compress(t, snowplowWrapper{
				FormatVersion:  1,
				PayloadVersion: 1,
				Records:        [][]byte{rawRecord},
			})

			goods, bads := decompress(Config{MaxBytesSinglePayload: 1000000}, compressedInput)
			require.Empty(t, bads)
			require.Len(t, goods, 1)
			assert.Equal(t, rawRecord, goods[0].Data)
		})
	}
}
