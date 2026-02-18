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

package failure

import (
	"github.com/snowplow/snowbridge/v3/pkg/models"
)

// FailureParser creates failure payloads from invalid and oversized messages
type FailureParser interface {
	MakeInvalidPayloads(messages []*models.Message) ([]*models.Message, error)
	MakeOversizedPayloads(maximumAllowedSizeBytes int, messages []*models.Message) ([]*models.Message, error)
}
