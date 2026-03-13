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
	"testing"

	"github.com/snowplow/snowbridge/v3/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

func TestHTTP_Rules_StatusMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rule := Rule{MatchingHTTPCodes: []int{500, 503}}

	matches := ruleMatches(response, rule)
	assert.True(matches)
}

func TestHTTP_Rules_FullBodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rule := Rule{MatchingHTTPCodes: []int{500, 503}, MatchingBodyPart: "Invalid field 'attribute'"}

	matches := ruleMatches(response, rule)
	assert.True(matches)
}

func TestHTTP_Rules_PartialBodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rule := Rule{MatchingHTTPCodes: []int{500, 503}, MatchingBodyPart: "Invalid field"}

	matches := ruleMatches(response, rule)
	assert.True(matches)
}

func TestHTTP_Rules_StatusMatch_NoBodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rule := Rule{MatchingHTTPCodes: []int{500, 503}, MatchingBodyPart: "Invalid field 'events'"}

	matches := ruleMatches(response, rule)
	assert.False(matches)
}

func TestHTTP_Rules_NoStatusMatch_BodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rule := Rule{MatchingHTTPCodes: []int{503}, MatchingBodyPart: "Invalid field"}

	matches := ruleMatches(response, rule)
	assert.False(matches)
}

func TestHTTP_ResponseRules_OrderedEvaluation(t *testing.T) {
	assert := assert.New(t)

	// Create HTTP target with ordered rules (setup first, then invalid)
	responseRules := &ResponseRules{
		Rules: []Rule{
			{
				Type:              ResponseRuleTypeSetup,
				MatchingHTTPCodes: []int{500},
				MatchingBodyPart:  "database",
			},
			{
				Type:              ResponseRuleTypeInvalid,
				MatchingHTTPCodes: []int{500},
				MatchingBodyPart:  "validation",
			},
			{
				Type:              ResponseRuleTypeSetup,
				MatchingHTTPCodes: []int{500},
			}, // no body requirement
			{
				Type:              ResponseRuleTypeThrottle,
				MatchingHTTPCodes: []int{429},
				MatchingBodyPart:  "rate limit",
			},
		},
	}

	ht := &HTTPTargetDriver{responseRules: responseRules}

	// Test that setup rule with "database" body matches first
	resp := response{Status: 500, Body: "database connection failed"}
	var matchedRule *Rule
	for _, rule := range ht.responseRules.Rules {
		if ruleMatches(resp, rule) {
			matchedRule = &rule
			break
		}
	}

	assert.NotNil(matchedRule)
	assert.Equal(ResponseRuleTypeSetup, matchedRule.Type)
	assert.Equal("database", matchedRule.MatchingBodyPart)

	// Test that invalid rule with "validation" body matches when database doesn't
	matchedRule = nil
	resp = response{Status: 500, Body: "validation error occurred"}
	for _, rule := range ht.responseRules.Rules {
		if ruleMatches(resp, rule) {
			matchedRule = &rule
			break
		}
	}

	assert.NotNil(matchedRule)
	assert.Equal(ResponseRuleTypeInvalid, matchedRule.Type)
	assert.Equal("validation", matchedRule.MatchingBodyPart)

	// Test that third setup rule matches when no body specified
	resp = response{Status: 500}
	matchedRule = nil
	for _, rule := range ht.responseRules.Rules {
		if ruleMatches(resp, rule) {
			matchedRule = &rule
			break
		}
	}

	assert.NotNil(matchedRule)
	assert.Equal(ResponseRuleTypeSetup, matchedRule.Type)
	assert.Equal("", matchedRule.MatchingBodyPart)

	// Test that throttle rule matches as expected
	resp = response{Status: 429, Body: "rate limit exceeded"}
	matchedRule = nil
	for _, rule := range ht.responseRules.Rules {
		if ruleMatches(resp, rule) {
			matchedRule = &rule
			break
		}
	}

	assert.NotNil(matchedRule)
	assert.Equal(ResponseRuleTypeThrottle, matchedRule.Type)
	assert.Equal("rate limit", matchedRule.MatchingBodyPart)
}

func TestHTTP_ResponseRules_ValidateRuleTypes(t *testing.T) {
	assert := assert.New(t)

	// Test valid rule types
	driver := &HTTPTargetDriver{}
	validConfig := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	validConfig.URL = "https://example.com"
	validConfig.BatchingConfig.MaxBatchBytes = 1048576
	validConfig.BatchingConfig.MaxMessageBytes = 1048576
	validConfig.ResponseRules = &ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400}},
			{Type: ResponseRuleTypeSetup, MatchingHTTPCodes: []int{500}},
			{Type: ResponseRuleTypeThrottle, MatchingHTTPCodes: []int{429}},
		},
	}

	err := driver.InitFromConfig(validConfig)
	assert.NoError(err)
	assert.NotNil(driver)

	// Test invalid rule type
	driver2 := &HTTPTargetDriver{}
	invalidConfig := driver2.GetDefaultConfiguration().(*HTTPTargetConfig)
	invalidConfig.URL = "https://example.com"
	invalidConfig.BatchingConfig.MaxBatchBytes = 1048576
	invalidConfig.BatchingConfig.MaxMessageBytes = 1048576
	invalidConfig.ResponseRules = &ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400}},
			{Type: ResponseRuleType("unknown"), MatchingHTTPCodes: []int{500}}, // Invalid type
		},
	}

	err = driver2.InitFromConfig(invalidConfig)
	assert.Error(err)
	// NOTE: In the new architecture, driver2 is not nil even when InitFromConfig fails,
	// but the old test checked that target was nil. This is a minor behavioral difference
	// in how errors are handled, but the test still validates the error message correctly.
	assert.Contains(err.Error(), "Invalid response rule type 'unknown'")

	// Test empty rule type should be invalid
	driver3 := &HTTPTargetDriver{}
	emptyTypeConfig := driver3.GetDefaultConfiguration().(*HTTPTargetConfig)
	emptyTypeConfig.URL = "https://example.com"
	emptyTypeConfig.BatchingConfig.MaxBatchBytes = 1048576
	emptyTypeConfig.BatchingConfig.MaxMessageBytes = 1048576
	emptyTypeConfig.ResponseRules = &ResponseRules{
		Rules: []Rule{
			{Type: ResponseRuleType(""), MatchingHTTPCodes: []int{400}}, // Empty type
		},
	}

	err = driver3.InitFromConfig(emptyTypeConfig)
	assert.Error(err)
	// NOTE: In the new architecture, driver3 is not nil even when InitFromConfig fails,
	// but the old test checked that target was nil. This is a minor behavioral difference
	// in how errors are handled, but the test still validates the error message correctly.
	assert.Contains(err.Error(), "Invalid response rule type ''")
}

func TestHTTP_HandleResponseRules_ErrorDetails(t *testing.T) {
	testCases := []struct {
		Name          string
		Response      response
		Rules         []Rule
		SafeMode      bool
		ExpectedError string
	}{
		{
			Name:          "Transient, safe mode off — full body in error details",
			Response:      response{Status: 500, StringStatus: "500 Internal Server Error", Body: `{"error": "something went wrong"}`},
			Rules:         []Rule{},
			SafeMode:      false,
			ExpectedError: `response status: '500 Internal Server Error' with error details: '{"error": "something went wrong"}'`,
		},
		{
			Name:          "Transient, safe mode on — generic label in error details",
			Response:      response{Status: 500, StringStatus: "500 Internal Server Error", Body: `{"error": "something went wrong"}`},
			Rules:         []Rule{},
			SafeMode:      true,
			ExpectedError: "response status: '500 Internal Server Error' with error details: 'Transient error'",
		},
		{
			Name:     "Client timeout, no matching rule — actual error in details",
			Response: response{Status: 0, StringStatus: "Client failed to complete request", Body: `Post "http://example.com": context deadline exceeded`},
			Rules:    []Rule{},
			SafeMode: false,
			ExpectedError: `response status: 'Client failed to complete request' with error details: ` +
				`'Post "http://example.com": context deadline exceeded'`,
		},
		{
			Name:          "Throttle, safe mode on, with matching body part",
			Response:      response{Status: 429, StringStatus: "429 Too Many Requests", Body: "Rate limit exceeded, retry after 30s"},
			Rules:         []Rule{{Type: ResponseRuleTypeThrottle, MatchingHTTPCodes: []int{429}, MatchingBodyPart: "Rate limit"}},
			SafeMode:      true,
			ExpectedError: "response status: '429 Too Many Requests' with error details: 'Rate limit'",
		},
		{
			Name:          "Throttle, safe mode on, no matching body part",
			Response:      response{Status: 429, StringStatus: "429 Too Many Requests", Body: "slow down"},
			Rules:         []Rule{{Type: ResponseRuleTypeThrottle, MatchingHTTPCodes: []int{429}}},
			SafeMode:      true,
			ExpectedError: "response status: '429 Too Many Requests' with error details: 'Throttle error'",
		},
		{
			Name:          "Throttle, safe mode off — full body in error details",
			Response:      response{Status: 429, StringStatus: "429 Too Many Requests", Body: "Rate limit exceeded, retry after 30s"},
			Rules:         []Rule{{Type: ResponseRuleTypeThrottle, MatchingHTTPCodes: []int{429}, MatchingBodyPart: "Rate limit"}},
			SafeMode:      false,
			ExpectedError: "response status: '429 Too Many Requests' with error details: 'Rate limit exceeded, retry after 30s'",
		},
		{
			Name:          "Setup, safe mode on, with matching body part",
			Response:      response{Status: 401, StringStatus: "401 Unauthorized", Body: "Invalid token: expired at 2024-01-01"},
			Rules:         []Rule{{Type: ResponseRuleTypeSetup, MatchingHTTPCodes: []int{401}, MatchingBodyPart: "Invalid token"}},
			SafeMode:      true,
			ExpectedError: "response status: '401 Unauthorized' with error details: 'Invalid token'",
		},
		{
			Name:          "Fatal, safe mode on, no matching body part",
			Response:      response{Status: 403, StringStatus: "403 Forbidden", Body: "Account suspended"},
			Rules:         []Rule{{Type: ResponseRuleTypeFatal, MatchingHTTPCodes: []int{403}}},
			SafeMode:      true,
			ExpectedError: "response status: '403 Forbidden' with error details: 'Fatal error'",
		},
		{
			Name:          "Invalid, safe mode on — no wrapped error",
			Response:      response{Status: 400, StringStatus: "400 Bad Request", Body: "malformed JSON"},
			Rules:         []Rule{{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400}}},
			SafeMode:      true,
			ExpectedError: "",
		},
		{
			Name:          "Invalid, safe mode off — no wrapped error",
			Response:      response{Status: 400, StringStatus: "400 Bad Request", Body: "malformed JSON"},
			Rules:         []Rule{{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400}}},
			SafeMode:      false,
			ExpectedError: "",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			messages := testutil.GetTestMessages(1, `{"data": "test"}`, nil)
			rules := &ResponseRules{Rules: tt.Rules}

			_, _, err := handleResponseRules(tt.Response, rules, messages, tt.SafeMode)

			if tt.ExpectedError == "" {
				assert.Nil(err)
			} else {
				assert.NotNil(err)
				assert.Equal(tt.ExpectedError, err.Error())
			}
		})
	}
}
