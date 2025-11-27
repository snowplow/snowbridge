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

// SetupWriteError is a wrapper for target write error. It is used by any target as a signal for a caller that this kind of error should be retried using 'setup-like' retry strategy.
type SetupWriteError struct {
	Err error
}

func (err SetupWriteError) Error() string {
	return err.Err.Error()
}

// ThrottleWriteError is a wrapper for target write error. It is used by any target as a signal for a caller that this kind of error should be retried using 'throttle-like' retry strategy.
type ThrottleWriteError struct {
	Err error
}

func (err ThrottleWriteError) Error() string {
	return err.Err.Error()
}
