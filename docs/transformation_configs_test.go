// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"path/filepath"
	"testing"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
	"github.com/snowplow-devops/stream-replicator/pkg/transform/transformconfig"
	"github.com/stretchr/testify/assert"
)

// TODO: These tests are gonna be limited until we refactor transformation config.

func TestSpEnrichedFilterConfig(t *testing.T) {
	assert := assert.New(t)

	hclFilename := filepath.Join("configs", "transformations", "snowplow-builtin", "full", "spEnrichedFilter-full.hcl")
	t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", hclFilename)

	c, err := config.NewConfig()
	assert.NotNil(c)
	if err != nil {
		t.Fatalf("function NewConfig failed with error: %q", err.Error())
	}

	transFunc, err := transformconfig.GetTransformations(c)
	assert.Nil(err)

	// TODO: Move test data somewhere more sensible, to be used everywhere
	res := transFunc(transform.Messages)

	// TODO: update the test data so that sensible use cases can be tested (some web/mobile events)
	// So we have some filtered and some unfiltered here.
	// This is likely to impact lots of tests.

	assert.Equal(3, len(res.Filtered))
	assert.Equal(0, len(res.Result))
	assert.Equal(1, len(res.Invalid))
}
