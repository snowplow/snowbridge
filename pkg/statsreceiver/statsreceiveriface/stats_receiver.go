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

package statsreceiveriface

import (
	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// StatsReceiver describes the interface for how to push observed statistics
// to a downstream store
type StatsReceiver interface {
	Send(buffer *models.ObserverBuffer)
}
