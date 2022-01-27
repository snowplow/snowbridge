// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package sourceiface

import (
	"github.com/snowplow-devops/stream-replicator/pkg/models"
)

// SourceFunctions contain the callback functions required by each source
type SourceFunctions struct {
	WriteToTarget func(messages []*models.Message) error
}
