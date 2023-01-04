//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package statsreceiveriface

import (
	"github.com/snowplow/snowbridge/pkg/models"
)

// StatsReceiver describes the interface for how to push observed statistics
// to a downstream store
type StatsReceiver interface {
	Send(buffer *models.ObserverBuffer)
}
