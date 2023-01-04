//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package sourceiface

import (
	"github.com/snowplow/snowbridge/pkg/models"
)

// SourceFunctions contain the callback functions required by each source
type SourceFunctions struct {
	WriteToTarget func(messages []*models.Message) error
}
