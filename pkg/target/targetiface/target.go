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

package targetiface

import (
	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// Target describes the interface for how to push the data pulled from the source
type Target interface {
	Write(messages []*models.Message) (*models.TargetWriteResult, error)
	Open()
	Close()
	MaximumAllowedMessageSizeBytes() int
	GetID() string
}
