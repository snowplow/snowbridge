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

package gcp

const (
	solutionURN = "isol_plb32_0014m00002tg62aqad_qufvepzajwfju6jxl5uwg2zn3l3nizte"

	// UserAgent is sent with every GCP API request from Snowbridge.
	UserAgent = "Google/ISV Solution (GPN:" + solutionURN + ")"
)
