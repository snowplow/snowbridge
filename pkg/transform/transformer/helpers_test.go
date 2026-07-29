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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/snowplow/snowbridge/v6/pkg/decompression"
	"github.com/snowplow/snowbridge/v6/pkg/models"
	"github.com/snowplow/snowbridge/v6/pkg/observer"
)

// passthroughTransform is a no-op transform: returns the message unchanged as a Transformed result.
var passthroughTransform = func(msg *models.Message) *models.TransformationResult {
	return models.NewTransformationResult(msg, nil, nil)
}

// runTransformer creates a 1-worker Transformer, feeds msgs into it, closes the input channel,
// waits for all workers to finish, then drains and returns all output results.
func runTransformer(
	t *testing.T,
	cfg decompression.Config,
	fn func(*models.Message) *models.TransformationResult,
	msgs []*models.Message,
	bufSize int,
) []*models.TransformationResult {
	t.Helper()

	input := make(chan *models.Message)
	output := make(chan *models.TransformationResult, bufSize)
	obs := observer.New(nil, 1*time.Second, nil)

	tr := NewTransformer(fn, cfg, input, output, obs, 1)

	var wg sync.WaitGroup
	wg.Go(tr.Start)

	for _, msg := range msgs {
		input <- msg
	}
	close(input)

	ok := waitWithTimeout(&wg)
	require.True(t, ok, "transformer did not complete within timeout")

	var results []*models.TransformationResult
	for result := range output {
		results = append(results, result)
	}
	return results
}
