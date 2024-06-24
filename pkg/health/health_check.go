/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package health 

import (
  "sync/atomic"
)

var isHealthy atomic.Bool

func SetHealthy() {
  isHealthy.Store(true)
}

func SetUnhealthy() {
  isHealthy.Store(false)
}

func IsHealthy() bool {
  return isHealthy.Load()
}
