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

import "fmt"

// Very specific error types...

// Target oriented...
type TargetTemplatingError struct {
	Err error
}

func (err TargetTemplatingError) Error() string {
	return fmt.Sprintf("Templating failed, error message: %s", err.Error())
}

type TargetApiError struct {
	Code string //"400 Bad Request"/"401 Unauthorized"/...
	Err  error
}

func (err TargetApiError) Error() string {
	return fmt.Sprintf("Error code :%s, message %s", err.Code, err.Error())
}

// Transformation orientied...
type JSRuntimeError struct {
	Code string
	Err  error
}

func (err JSRuntimeError) Error() string {
	return err.Err.Error()
}

type SpEnrichedToJsonError struct {
	Err error
}

func (err SpEnrichedToJsonError) Error() string {
	return err.Err.Error()
}

/// add more...

//OR

// More generic error types with codes/categories provided by component producing an error. Might be API/templating or whatever can fail in a target

type TargetError struct {
	Code string //"Templating"/"API 400 Bad Request"/"401 Unauthorized"....
	Err  error
}

func (err TargetError) Error() string {
	return fmt.Sprintf("Error code :%s, message %s", err.Code, err.Error())
}

type TransformationError struct {
	Code string // JSRuntime/SpEnrichedToJson/....
	Err  error
}

func (err TransformationError) Error() string {
	return err.Err.Error()
}
