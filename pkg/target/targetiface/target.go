// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package targetiface

import (
	"github.com/snowplow/snowbridge/pkg/models"
)

// Target describes the interface for how to push the data pulled from the source
type Target interface {
	Write(messages []*models.Message) (*models.TargetWriteResult, error)
	Open()
	Close()
	MaximumAllowedMessageSizeBytes() int
	GetID() string
}
