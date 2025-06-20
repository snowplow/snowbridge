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

package models

// ErrorMetadata is an interface which could be implemented by errors produced by various Snowbridge components.
// If an error implements this interface, it has to provide code and description that is safe to report as metadata.
type ErrorMetadata interface {
	ReportableCode() string
	ReportableDescription() string
}
