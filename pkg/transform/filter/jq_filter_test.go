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

package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/snowplow/snowbridge/pkg/transform"
)

func TestJQFilter_SpMode_true_keep(t *testing.T) {
	assert := assert.New(t)
	input := &models.Message{
		Data:         transform.SnowplowTsv1,
		PartitionKey: "some-key",
	}

	config := &JQFilterConfig{JQCommand: `has("app_id")`, RunTimeoutMs: 100, SpMode: true}
	filter := createFilter(t, config)

	kept, dropped, invalid, _ := filter(input, nil)
	assert.Empty(dropped)
	assert.Empty(invalid)
	assert.Equal(string(transform.SnowplowTsv1), string(kept.Data))
}

func TestJQFilter_SpMode_true_drop(t *testing.T) {
	assert := assert.New(t)
	input := &models.Message{
		Data:         transform.SnowplowTsv1,
		PartitionKey: "some-key",
	}

	config := &JQFilterConfig{JQCommand: `has("non_existent_key")`, RunTimeoutMs: 100, SpMode: true}
	filter := createFilter(t, config)

	kept, dropped, invalid, _ := filter(input, nil)
	assert.Empty(kept)
	assert.Empty(invalid)
	assert.Equal(string(transform.SnowplowTsv1), string(dropped.Data))
}

func TestJQFilter_SpMode_false_keep(t *testing.T) {
	assert := assert.New(t)
	input := &models.Message{
		Data:         transform.SnowplowJSON1,
		PartitionKey: "some-key",
	}

	config := &JQFilterConfig{JQCommand: `has("app_id")`, RunTimeoutMs: 100, SpMode: false}
	filter := createFilter(t, config)

	kept, dropped, invalid, _ := filter(input, nil)
	assert.Empty(dropped)
	assert.Empty(invalid)
	assert.Equal(string(transform.SnowplowJSON1), string(kept.Data))
}

func TestJQFilter_SpMode_false_drop(t *testing.T) {
	assert := assert.New(t)
	input := &models.Message{
		Data:         transform.SnowplowJSON1,
		PartitionKey: "some-key",
	}

	config := &JQFilterConfig{JQCommand: `has("non_existent_key")`, RunTimeoutMs: 100, SpMode: false}
	filter := createFilter(t, config)

	kept, dropped, invalid, _ := filter(input, nil)
	assert.Empty(kept)
	assert.Empty(invalid)
	assert.Equal(string(transform.SnowplowJSON1), string(dropped.Data))
}

func TestJQFilter_epoch(t *testing.T) {
	assert := assert.New(t)
	input := &models.Message{
		Data:         transform.SnowplowTsv1,
		PartitionKey: "some-key",
	}

	config := &JQFilterConfig{JQCommand: `.collector_tstamp | epoch | . < 10`, RunTimeoutMs: 100, SpMode: true}
	filter := createFilter(t, config)

	kept, dropped, invalid, _ := filter(input, nil)
	assert.Empty(kept)
	assert.Empty(invalid)
	assert.Equal(string(transform.SnowplowTsv1), string(dropped.Data))
}

func TestJQFilter_non_boolean_output(t *testing.T) {
	assert := assert.New(t)
	input := &models.Message{
		Data:         transform.SnowplowTsv1,
		PartitionKey: "some-key",
	}

	config := &JQFilterConfig{JQCommand: `.collector_tstamp | epoch`, RunTimeoutMs: 100, SpMode: true}
	filter := createFilter(t, config)

	kept, dropped, invalid, _ := filter(input, nil)

	assert.Empty(kept)
	assert.Empty(dropped)
	assert.Equal("jq filter didn't return expected [boolean] value: 1557499235", invalid.GetError().Error())
}

func TestJQFilter_invalid_jq_command(t *testing.T) {
	assert := assert.New(t)

	config := &JQFilterConfig{JQCommand: `blabla`, RunTimeoutMs: 100, SpMode: true}
	filter, err := jqFilterConfigFunction(config)

	assert.Nil(filter)
	assert.Equal("error compiling jq query: function not defined: blabla/0", err.Error())
}

func createFilter(t *testing.T, config *JQFilterConfig) transform.TransformationFunction {
	filter, err := jqFilterConfigFunction(config)
	if err != nil {
		t.Fatalf("failed to create transformation function with error: %q", err.Error())
	}
	return filter
}
