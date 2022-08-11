// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

func TestNewSpEnrichedAddMetadata(t *testing.T) {
	assert := assert.New(t)

	var messageGood = models.Message{
		Data:         snowplowTsv3,
		PartitionKey: "some-key",
	}

	var messageBad = models.Message{
		Data:         nonSnowplowString,
		PartitionKey: "some-key4",
	}

	aidAddMetadata := NewSpEnrichedAddMetadataFunction("test-key", "app_id")

	res, _, fail, intermediate := aidAddMetadata(&messageGood, nil)

	assert.Equal("test-data3", res.Metadata["test-key"])
	assert.Equal(spTsv3Parsed, intermediate)
	assert.Nil(fail)

	res, _, fail, intermediate = aidAddMetadata(&messageBad, nil)

	assert.Nil(res)
	assert.Nil(intermediate)
	assert.NotNil(fail)
	assert.NotNil(fail.GetError())
	if fail.GetError() != nil {
		assert.Equal("Cannot parse tsv event - wrong number of fields provided: 4", fail.GetError().Error())
	}

	ctstampAddMetadata := NewSpEnrichedAddMetadataFunction("test-key", "collector_tstamp")

	tstampRes, _, fail, intermediate := ctstampAddMetadata(&messageGood, nil)

	assert.Equal("2019-05-10 14:40:29.576 +0000 UTC", tstampRes.Metadata["test-key"])
	assert.Equal(spTsv3Parsed, intermediate)
	assert.Nil(fail)

	pgurlportAddMetadata := NewSpEnrichedAddMetadataFunction("test-key", "page_urlport")

	intRes, _, fail, intermediate := pgurlportAddMetadata(&messageGood, nil)

	assert.Equal("80", intRes.Metadata["test-key"])
	assert.Equal(spTsv3Parsed, intermediate)
	assert.Nil(fail)
}
