//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

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
