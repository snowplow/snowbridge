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

package http

import (
	"errors"
	"fmt"
)

// ConnectionTestConfig configures the connection test for the HTTP target
type ConnectionTestConfig struct {
	RequestBody        string `hcl:"request_body"`
	ExpectedStatusCode int    `hcl:"expected_status_code"`
}

// ConnectionCheckResult holds the outcome of a connection test
type ConnectionCheckResult struct {
	Host     string
	Complete bool
	Messages []string
}

// CheckConnection sends the configured request body to the configured URL, using the configured
// client, headers and authentication. It passes if the expected response code is received.
// The response body is never read, as destinations may echo credentials back in it.
func (ht *HTTPTargetDriver) CheckConnection() (ConnectionCheckResult, error) {
	if ht.connectionTest == nil {
		return ConnectionCheckResult{}, errors.New("http target has no connection_test configured")
	}

	result := ConnectionCheckResult{Host: ht.httpURL, Complete: false, Messages: []string{}}

	request, _, err := ht.newRequest([]byte(ht.connectionTest.RequestBody), nil)
	if err != nil {
		return ConnectionCheckResult{}, err
	}

	resp, err := ht.client.Do(request)
	if err != nil {
		result.Messages = append(result.Messages, fmt.Sprintf("request failed: %s", err))
		return result, nil
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			ht.log.Error(err.Error())
		}
	}()

	if resp.StatusCode == ht.connectionTest.ExpectedStatusCode {
		result.Complete = true
		return result, nil
	}

	result.Messages = append(result.Messages, fmt.Sprintf("status %s", resp.Status))
	return result, nil
}
