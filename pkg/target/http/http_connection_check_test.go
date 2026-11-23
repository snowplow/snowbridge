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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTP_CheckConnection(t *testing.T) {
	testCases := []struct {
		Name             string
		ResponseCode     int
		ResponseBody     string
		ExpectedComplete bool
		ExpectedMessages []string
	}{
		{
			Name:             "expected response code",
			ResponseCode:     400,
			ResponseBody:     `{"error":"no events"}`,
			ExpectedComplete: true,
			ExpectedMessages: []string{},
		},
		{
			Name:             "unexpected response code",
			ResponseCode:     401,
			ResponseBody:     `{"error":"Invalid API key: secret-key"}`,
			ExpectedComplete: false,
			ExpectedMessages: []string{"status 401 Unauthorized"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			var results [][]byte
			var headers http.Header
			server := createTestServerWithResponseCode(&results, &headers, tt.ResponseCode, tt.ResponseBody, 0)
			defer server.Close()

			driver := &HTTPTargetDriver{}
			config := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
			config.URL = server.URL
			config.ConnectionTest = &ConnectionTestConfig{
				RequestBody:        `{"events": []}`,
				ExpectedStatusCode: 400,
			}
			if err := driver.InitFromConfig(config); err != nil {
				t.Fatal(err)
			}

			result, err := driver.CheckConnection()

			assert.Nil(err)
			assert.Equal(server.URL, result.Host)
			assert.Equal(tt.ExpectedComplete, result.Complete)
			assert.Equal(tt.ExpectedMessages, result.Messages)
			assert.Equal([][]byte{[]byte(`{"events": []}`)}, results)
		})
	}
}
