// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package failureiface

import (
	"github.com/snowplow/snowbridge/pkg/models"
)

// Failure describes the interface for where to push data that
// cannot be sent to its desired target and therefore should no longer be retried.
//
// This can be for:
// 1. Invalid messages
// 2. Oversized messages
type Failure interface {
	WriteInvalid(messages []*models.Message) (*models.TargetWriteResult, error)
	WriteOversized(maximumAllowedSizeBytes int, messages []*models.Message) (*models.TargetWriteResult, error)
	Open()
	Close()
	GetID() string
}
