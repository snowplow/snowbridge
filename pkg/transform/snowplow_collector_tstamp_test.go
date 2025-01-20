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
	"testing"
	"time"

	"github.com/snowplow/snowbridge/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestCollectorTstamp_Snowplow_Data(t *testing.T) {
	assert := assert.New(t)

	input := models.Message{
		Data:         SnowplowTsv1,
		PartitionKey: "some-key",
	}

	ts := CollectorTstampTransformation()

	good, filtered, invalid, _ := ts(&input, nil)

	assert.Equal(time.Date(2019, 5, 10, 14, 40, 35, 972000000, time.UTC), good.CollectorTstamp)
	assert.Empty(filtered)
	assert.Empty(invalid)
}

func TestCollectorTstamp_Non_Snowplow_Data(t *testing.T) {
	assert := assert.New(t)

	input := &models.Message{
		Data:         []byte("Some kind of custom non-Snowplow data"),
		PartitionKey: "some-key",
	}

	ts := CollectorTstampTransformation()

	good, filtered, invalid, _ := ts(input, nil)

	assert.Equal(input, good)
	assert.Empty(good.CollectorTstamp)
	assert.Empty(filtered)
	assert.Empty(invalid)
}
