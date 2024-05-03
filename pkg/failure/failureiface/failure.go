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
