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

import (
	"fmt"
)

// ErrorMetadata is an interface which could be implemented by errors produced by various Snowbridge components.
// If an error implements this interface, it has to provide code and description that is safe to report as metadata.
type ErrorMetadata interface {
	ReportableCode() string
	ReportableDescription() string
	ReportableType() string
}

const (
	ErrorTypeAPI            = "api"
	ErrorTypeTransformation = "transformation"
	ErrorTypeTemplate       = "template"
)

type TransformationError struct {
	SafeMessage string
	Err         error
}

func (e *TransformationError) Error() string {
	return e.Err.Error()
}

func (e *TransformationError) ReportableCode() string {
	return ""
}

func (e *TransformationError) ReportableDescription() string {
	return e.SafeMessage
}

func (e *TransformationError) ReportableType() string {
	return ErrorTypeTransformation
}

type ApiError struct {
	HttpStatus   string
	ResponseBody string
}

func (e *ApiError) Error() string {
	return fmt.Sprintf("HTTP Status Code: %s Body: %s", e.HttpStatus, e.ResponseBody)
}

func (e *ApiError) ReportableCode() string {
	return e.HttpStatus
}

func (e *ApiError) ReportableDescription() string {
	return e.ResponseBody
}

func (e *ApiError) ReportableType() string {
	return ErrorTypeAPI
}

type TemplatingError struct {
	SafeMessage string
	Err         error
}

func (e *TemplatingError) Error() string {
	return e.Err.Error()
}

func (e *TemplatingError) ReportableCode() string {
	return ""
}

func (e *TemplatingError) ReportableDescription() string {
	return e.SafeMessage
}

func (e *TemplatingError) ReportableType() string {
	return ErrorTypeTemplate
}
