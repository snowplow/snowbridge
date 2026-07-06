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

// Package decompression implements the Snowplow compressed stream-record format.
package decompression

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Config controls memory protection during decompression.
type Config struct {
	// MaxBytesSinglePayload bounds individual record allocations. Records
	// over this size are skipped without being allocated.
	MaxBytesSinglePayload int
}

// DecompressedRecord is one successfully decompressed record plus the
// inner payload format version from the compression header.
type DecompressedRecord struct {
	Data []byte
	// PayloadVersion is the second byte of the Snowplow compression header,
	// telling consumers how to interpret Data (e.g. Thrift binary for raw,
	// JSON for enriched). Nil for uncompressed inputs — they have no header.
	PayloadVersion *int
}

// BadRecord describes a single decompression failure.
type BadRecord struct {
	Kind BadRecordKind
	// Message is a payload-free description safe to emit as metadata. It never
	// embeds bytes from the input or wrapped external errors.
	Message string
	// Err holds detail that belongs in the failure payload but not in
	// metadata: the underlying compression-library or IO error, or values that
	// would widen metric cardinality (e.g. an actual record size). May be nil.
	Err error
}

// BadRecordKind enumerates decompression failure modes.
type BadRecordKind int

const (
	// SizeViolation — a record exceeded Config.MaxBytesSinglePayload.
	SizeViolation BadRecordKind = iota
	// CorruptInput — malformed compressed data or truncated record.
	CorruptInput
	// UnsupportedVersion — unknown compression format version or codec.
	UnsupportedVersion
)

func (k BadRecordKind) String() string {
	switch k {
	case SizeViolation:
		return "SizeViolation"
	case CorruptInput:
		return "CorruptInput"
	case UnsupportedVersion:
		return "UnsupportedVersion"
	default:
		return "Unknown"
	}
}

// ResultKind discriminates between successful and failed records in the
// stream produced by Decompress.
type ResultKind int

const (
	// ResultRecord — Result.Record carries a successfully decompressed record.
	ResultRecord ResultKind = iota
	// ResultBad — Result.Bad describes a per-record failure.
	ResultBad
)

// Result is a single item yielded by Decompress. Exactly one of Record or
// Bad is meaningful, selected by Kind.
type Result struct {
	Kind   ResultKind
	Record DecompressedRecord
	Bad    BadRecord
}

// Decompress returns an iterator over the records in data. Successful
// records and per-record failures are interleaved in the order produced by
// the underlying stream. Corrupt input ends the stream; size violations do
// not. Stopping the range loop early closes the underlying reader.
func Decompress(cfg Config, data []byte) iter.Seq[Result] {
	return func(yield func(Result) bool) {
		if len(data) == 0 {
			return
		}

		switch sniff(data) {
		case compressionGzip:
			decompressGzip(cfg, data, yield)
		case compressionZstd:
			decompressZstd(cfg, data, yield)
		default:
			// Uncompressed — no header, no framing; emit the whole payload as-is.
			yield(Result{Kind: ResultRecord, Record: DecompressedRecord{Data: data}})
		}
	}
}

func decompressGzip(cfg Config, data []byte, yield func(Result) bool) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		yield(Result{Kind: ResultBad, Bad: BadRecord{
			Kind:    CorruptInput,
			Message: "gzip: failed to open reader",
			Err:     err,
		}})
		return
	}
	readSnowplowStream(cfg, gz, "gzip", yield)
}

// zstdDecoderPool stores reusable zstd decoders.
// As we decompressing data synchronously, concurrency is set to 1.
var zstdDecoderPool = sync.Pool{
	New: func() any {
		dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		return dec
	},
}

// pooledZstdReader wraps a *zstd.Decoder borrowed from zstdDecoderPool.
// Close resets and returns the decoder to the pool instead of destroying it.
type pooledZstdReader struct {
	dec *zstd.Decoder
}

func (r *pooledZstdReader) Read(p []byte) (int, error) { return r.dec.Read(p) }

func (r *pooledZstdReader) Close() error {
	// Reset(nil) releases the reference to the underlying reader before pooling.
	_ = r.dec.Reset(nil)
	zstdDecoderPool.Put(r.dec)
	r.dec = nil
	return nil
}

func decompressZstd(cfg Config, data []byte, yield func(Result) bool) {
	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	if err := dec.Reset(bytes.NewReader(data)); err != nil {
		_ = dec.Reset(nil)
		zstdDecoderPool.Put(dec)
		yield(Result{Kind: ResultBad, Bad: BadRecord{
			Kind:    CorruptInput,
			Message: "zstd: failed to open reader",
			Err:     err,
		}})
		return
	}
	readSnowplowStream(cfg, &pooledZstdReader{dec: dec}, "zstd", yield)
}

// readSnowplowStream drives the Snowplow header + record loop over an
// already-opened decompression reader. Closes the reader on return.
func readSnowplowStream(
	cfg Config,
	reader io.ReadCloser,
	algorithm string,
	yield func(Result) bool,
) {
	defer func() { _ = reader.Close() }()

	// 2-byte header: [compression format version][payload format version].
	var header [2]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		yield(Result{Kind: ResultBad, Bad: BadRecord{
			Kind:    CorruptInput,
			Message: fmt.Sprintf("%s: short header", algorithm),
			Err:     err,
		}})
		return
	}
	v1, v2 := int(header[0]), int(header[1])
	if v1 != 1 {
		yield(Result{Kind: ResultBad, Bad: BadRecord{
			Kind:    UnsupportedVersion,
			Message: fmt.Sprintf("%s: unsupported versions in compressed record header: %d, %d", algorithm, v1, v2),
		}})
		return
	}
	payloadVersion := v2

	rr := &recordReader{
		reader:                reader,
		maxBytesSinglePayload: cfg.MaxBytesSinglePayload,
	}

	for {
		bts, result, size := rr.next()
		switch result {
		case recordOK:
			if !yield(Result{Kind: ResultRecord, Record: DecompressedRecord{Data: bts, PayloadVersion: &payloadVersion}}) {
				return
			}

		case recordTooBig:
			if !yield(Result{Kind: ResultBad, Bad: BadRecord{
				Kind:    SizeViolation,

				// No actual record size in the message to keep cardinality low.
				Message: fmt.Sprintf("%s: record exceeds configured maximum size of %d bytes after decompression", algorithm, cfg.MaxBytesSinglePayload),
				Err:     fmt.Errorf("actual record size %d bytes", size),
			}}) {
				return
			}

		case recordCorrupt:
			yield(Result{Kind: ResultBad, Bad: BadRecord{
				Kind:    CorruptInput,
				Message: fmt.Sprintf("%s: corrupt compressed payload", algorithm),
			}})
			return

		case recordEnd:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Compression sniffing
// ---------------------------------------------------------------------------

type compression int

const (
	compressionNone compression = iota
	compressionGzip
	compressionZstd
)

// Magic byte signatures. Short buffers or non-matches are treated as uncompressed.
var (
	zstdSignature = []byte{0x28, 0xb5, 0x2f, 0xfd}
	gzipSignature = []byte{0x1f, 0x8b}
)

func sniff(data []byte) compression {
	if len(data) >= len(zstdSignature) && bytes.Equal(data[:len(zstdSignature)], zstdSignature) {
		return compressionZstd
	}
	if len(data) >= len(gzipSignature) && bytes.Equal(data[:len(gzipSignature)], gzipSignature) {
		return compressionGzip
	}
	return compressionNone
}

// ---------------------------------------------------------------------------
// Per-record reader
// ---------------------------------------------------------------------------

type recordReader struct {
	reader                io.Reader
	maxBytesSinglePayload int
}

type recordResult int

const (
	recordOK recordResult = iota
	// recordEnd — clean EOF.
	recordEnd
	// recordTooBig — declared size exceeded the limit; bytes were skipped.
	recordTooBig
	// recordCorrupt — IO error, truncated size header, or partial record.
	recordCorrupt
)

// next reads the next record. On recordOK the []byte holds the record and
// size is its length. On recordTooBig size is the declared (skipped) size
// and []byte is nil. Otherwise both are zero-valued.
func (r *recordReader) next() ([]byte, recordResult, int) {
	var sizeBytes [4]byte
	_, err := io.ReadFull(r.reader, sizeBytes[:])
	switch {
	case errors.Is(err, io.EOF):
		return nil, recordEnd, 0
	case errors.Is(err, io.ErrUnexpectedEOF):
		return nil, recordCorrupt, 0
	case err != nil:
		return nil, recordCorrupt, 0
	}

	// Reject negative int32 sizes — guards against wraparound on 32-bit
	// and malformed headers.
	size := int32(binary.BigEndian.Uint32(sizeBytes[:]))
	if size < 0 {
		return nil, recordCorrupt, 0
	}
	n := int(size)

	if n > r.maxBytesSinglePayload {
		// Skip without allocating.
		if _, err := io.CopyN(io.Discard, r.reader, int64(n)); err != nil {
			return nil, recordCorrupt, 0
		}
		return nil, recordTooBig, n
	}

	buf := make([]byte, n)
	if _, err := io.ReadFull(r.reader, buf); err != nil {
		return nil, recordCorrupt, 0
	}
	return buf, recordOK, n
}
