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

package transform

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/snowplow/snowbridge/v6/pkg/decompression"
	"github.com/snowplow/snowbridge/v6/pkg/models"
	"github.com/snowplow/snowbridge/v6/pkg/testutil"
	"github.com/snowplow/snowbridge/v6/pkg/transform"
)

// TestTransformer_Decompression_Zstd_MultiRecord_HappyPath verifies that a zstd-compressed
// payload fans out to N Transformed children carrying decompressed bytes, and that the parent
// acks only after every child acks.
func TestTransformer_Decompression_Zstd_MultiRecord_HappyPath(t *testing.T) {
	parentAcked := make(chan struct{})
	records := [][]byte{transform.SnowplowTsv1, []byte("rec-b"), []byte("rec-c")}
	compressed := testutil.ZstdCompressPayload(t, 1, 1, records)

	msg := &models.Message{
		Data:         compressed,
		PartitionKey: "key",
		TimePulled:   time.Now(),
		AckFunc:      func() { close(parentAcked) },
	}

	results := runTransformer(t, testDecompressionCfg, passthroughTransform, []*models.Message{msg}, 3)
	require.Len(t, results, 3)
	for i, rec := range records {
		require.NotNil(t, results[i].Transformed, "result %d should be Transformed", i)
		assert.Equal(t, rec, results[i].Transformed.Data, "result %d data mismatch", i)
	}

	results[0].Transformed.AckFunc()
	select {
	case <-parentAcked:
		t.Fatal("parent acked after only 1 of 3 children")
	default:
	}

	results[1].Transformed.AckFunc()
	select {
	case <-parentAcked:
		t.Fatal("parent acked after only 2 of 3 children")
	default:
	}

	results[2].Transformed.AckFunc()
	select {
	case <-parentAcked:
	case <-time.After(time.Second):
		t.Fatal("parent not acked after all 3 children acked")
	}
}

// TestTransformer_Decompression_Gzip_MultiRecord_HappyPath verifies that gzip-compressed
// payloads route through the same wiring as zstd and fan out to N Transformed children.
func TestTransformer_Decompression_Gzip_MultiRecord_HappyPath(t *testing.T) {
	records := [][]byte{
		transform.SnowplowTsv2,
		[]byte("gzip-rec-two"),
		[]byte("gzip-rec-three"),
	}
	compressed := testutil.GzipCompressPayload(t, 1, 1, records)

	msg := &models.Message{Data: compressed, PartitionKey: "key", TimePulled: time.Now()}
	results := runTransformer(t, testDecompressionCfg, passthroughTransform, []*models.Message{msg}, 3)

	require.Len(t, results, 3)
	for i, rec := range records {
		require.NotNil(t, results[i].Transformed, "result %d should be Transformed", i)
		assert.Equal(t, rec, results[i].Transformed.Data, "result %d data mismatch", i)
	}
}

// TestTransformer_Decompression_Uncompressed_PassesThrough verifies that non-compressed bytes
// reach the transform fn unchanged and go through fanout (not a shortcut), producing a 1:1 ack.
func TestTransformer_Decompression_Uncompressed_PassesThrough(t *testing.T) {
	parentAcked := make(chan struct{})
	data := transform.SnowplowTsv3

	msg := &models.Message{
		Data:         data,
		PartitionKey: "key",
		TimePulled:   time.Now(),
		AckFunc:      func() { close(parentAcked) },
	}

	results := runTransformer(t, testDecompressionCfg, passthroughTransform, []*models.Message{msg}, 1)
	require.Len(t, results, 1)
	require.NotNil(t, results[0].Transformed)
	assert.Equal(t, data, results[0].Transformed.Data)

	select {
	case <-parentAcked:
		t.Fatal("parent acked before child")
	default:
	}

	results[0].Transformed.AckFunc()
	select {
	case <-parentAcked:
	case <-time.After(time.Second):
		t.Fatal("parent not acked after child acked")
	}
}

// TestTransformer_Decompression_MixedGoodAndBad_AllParticipateInFanout verifies that
// Invalid children (from size violations) participate in fanout's ack count — the parent
// acks only after every child, including Invalid ones, resolves.
func TestTransformer_Decompression_MixedGoodAndBad_AllParticipateInFanout(t *testing.T) {
	const limit = 20
	parentAcked := make(chan struct{})

	good1 := []byte("good-one")
	tooBig := bytes.Repeat([]byte{0xAA}, limit+1)
	good2 := []byte("good-two")
	compressed := testutil.ZstdCompressPayload(t, 1, 1, [][]byte{good1, tooBig, good2})

	cfg := decompression.Config{MaxBytesSinglePayload: limit}
	msg := &models.Message{
		Data:         compressed,
		PartitionKey: "key",
		TimePulled:   time.Now(),
		AckFunc:      func() { close(parentAcked) },
	}

	results := runTransformer(t, cfg, passthroughTransform, []*models.Message{msg}, 3)
	require.Len(t, results, 3)
	require.NotNil(t, results[0].Transformed)
	assert.Equal(t, good1, results[0].Transformed.Data)
	require.NotNil(t, results[1].Invalid)
	assert.Nil(t, results[1].Transformed)
	require.NotNil(t, results[2].Transformed)
	assert.Equal(t, good2, results[2].Transformed.Data)

	// Ack the two Transformed children; Invalid child is still pending.
	results[0].Transformed.AckFunc()
	results[2].Transformed.AckFunc()
	select {
	case <-parentAcked:
		t.Fatal("parent acked before Invalid child resolved")
	default:
	}

	// Ack the Invalid child — parent should now fire.
	results[1].Invalid.AckFunc()
	select {
	case <-parentAcked:
	case <-time.After(time.Second):
		t.Fatal("parent not acked after all children resolved")
	}
}

// TestTransformer_Decompression_InvalidResult_Shape verifies the error prefix and base64
// encoding of the parent's data for each failure kind the decompressor can produce.
func TestTransformer_Decompression_InvalidResult_Shape(t *testing.T) {
	cases := []struct {
		name          string
		data          []byte
		cfg           decompression.Config
		wantPrefix    string
		wantCode      string
		wantSafe      string
		wantErrDetail string
	}{
		{
			name:       "UnsupportedVersion",
			data:       testutil.ZstdCompressPayload(t, 2, 1, [][]byte{[]byte("x")}),
			cfg:        testDecompressionCfg,
			wantPrefix: "decompression UnsupportedVersion:",
			wantCode:   "UnsupportedVersion",
			wantSafe:   "zstd: unsupported versions in compressed record header: 2, 1",
		},
		{
			name:       "CorruptInput",
			data:       testutil.ZstdCompressRawBytes(t, []byte{0x01}),
			cfg:        testDecompressionCfg,
			wantPrefix: "decompression CorruptInput:",
			wantCode:   "CorruptInput",
			wantSafe:   "zstd: short header",
		},
		{
			name:          "SizeViolation",
			data:          testutil.ZstdCompressPayload(t, 1, 1, [][]byte{bytes.Repeat([]byte{0xFF}, 101)}),
			cfg:           decompression.Config{MaxBytesSinglePayload: 100},
			wantPrefix:    "decompression SizeViolation:",
			wantCode:      "SizeViolation",
			wantSafe:      "zstd: record exceeds configured maximum size of 100 bytes after decompression",
			wantErrDetail: "actual record size 101 bytes",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &models.Message{Data: tc.data, PartitionKey: "key", TimePulled: time.Now()}
			results := runTransformer(t, tc.cfg, passthroughTransform, []*models.Message{msg}, 1)

			require.Len(t, results, 1)
			require.NotNil(t, results[0].Invalid)
			err := results[0].Invalid.GetError()
			assert.True(t, strings.HasPrefix(err.Error(), tc.wantPrefix),
				"error %q should have prefix %q", err.Error(), tc.wantPrefix)

			meta, ok := err.(models.SanitisedErrorMetadata)
			require.True(t, ok, "decompression error must implement SanitisedErrorMetadata")
			assert.Equal(t, tc.wantCode, meta.Code())
			assert.Equal(t, models.ErrorTypeDecompression, meta.Type())
			assert.Equal(t, tc.wantSafe, meta.SanitisedError())

			// Detail (e.g. actual size) stays in Error() and out of the sanitised metadata.
			if tc.wantErrDetail != "" {
				assert.Contains(t, err.Error(), tc.wantErrDetail)
				assert.NotContains(t, meta.SanitisedError(), tc.wantErrDetail)
			}

			// Invalid.Data must be the base64 encoding of the parent's original compressed bytes.
			expected := base64.StdEncoding.EncodeToString(tc.data)
			assert.Equal(t, []byte(expected), results[0].Invalid.Data)
		})
	}
}

// TestTransformer_Decompression_MultipleParents_Independent verifies that two compressed
// messages fan out independently, each parent acking separately after its own children resolve.
func TestTransformer_Decompression_MultipleParents_Independent(t *testing.T) {
	acked := [2]chan struct{}{make(chan struct{}), make(chan struct{})}

	msgs := make([]*models.Message, 2)
	for i := range 2 {
		records := [][]byte{
			fmt.Appendf(nil, "msg%d-rec0", i),
			fmt.Appendf(nil, "msg%d-rec1", i),
		}
		idx := i
		msgs[i] = &models.Message{
			Data:         testutil.ZstdCompressPayload(t, 1, 1, records),
			PartitionKey: fmt.Sprintf("key-%d", i),
			TimePulled:   time.Now(),
			AckFunc:      func() { close(acked[idx]) },
		}
	}

	results := runTransformer(t, testDecompressionCfg, passthroughTransform, msgs, 4)
	require.Len(t, results, 4)

	for _, result := range results {
		require.NotNil(t, result.Transformed)
		result.Transformed.AckFunc()
	}

	for i := range 2 {
		select {
		case <-acked[i]:
		case <-time.After(time.Second):
			t.Fatalf("parent %d not acked after all children acked", i)
		}
	}
}
