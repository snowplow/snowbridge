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

package telemetry

import (
	"time"

	"github.com/snowplow/snowbridge/v3/cmd"
)

var (
	interval           = time.Hour
	method             = "POST"
	protocol           = "https"
	url                = "telemetry-g.snowplowanalytics.com"
	port               = "443"
	applicationName    = cmd.AppName
	applicationVersion = cmd.AppVersion
)
