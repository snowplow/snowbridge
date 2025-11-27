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

package target

import (
	"testing"

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

	ht := &HTTPTarget{responseRules: responseRules}

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
	validConfig := &HTTPTargetConfig{
		URL:              "https://example.com",
		RequestByteLimit: 1048576,
		MessageByteLimit: 1048576,
		ResponseRules: &ResponseRules{
			Rules: []Rule{
				{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400}},
				{Type: ResponseRuleTypeSetup, MatchingHTTPCodes: []int{500}},
				{Type: ResponseRuleTypeThrottle, MatchingHTTPCodes: []int{429}},
			},
		},
	}

	target, err := HTTPTargetConfigFunction(validConfig)
	assert.NoError(err)
	assert.NotNil(target)

	// Test invalid rule type
	invalidConfig := &HTTPTargetConfig{
		URL:              "https://example.com",
		RequestByteLimit: 1048576,
		MessageByteLimit: 1048576,
		ResponseRules: &ResponseRules{
			Rules: []Rule{
				{Type: ResponseRuleTypeInvalid, MatchingHTTPCodes: []int{400}},
				{Type: ResponseRuleType("unknown"), MatchingHTTPCodes: []int{500}}, // Invalid type
			},
		},
	}

	target, err = HTTPTargetConfigFunction(invalidConfig)
	assert.Error(err)
	assert.Nil(target)
	assert.Contains(err.Error(), "Invalid response rule type 'unknown'")

	// Test empty rule type should be invalid
	emptyTypeConfig := &HTTPTargetConfig{
		URL:              "https://example.com",
		RequestByteLimit: 1048576,
		MessageByteLimit: 1048576,
		ResponseRules: &ResponseRules{
			Rules: []Rule{
				{Type: ResponseRuleType(""), MatchingHTTPCodes: []int{400}}, // Empty type
			},
		},
	}

	target, err = HTTPTargetConfigFunction(emptyTypeConfig)
	assert.Error(err)
	assert.Nil(target)
	assert.Contains(err.Error(), "Invalid response rule type ''")
}
