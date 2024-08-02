/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
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
	rules := []Rule{
		{MatchingHTTPCodes: []int{500, 503}},
	}

	matchingRule := findMatchingRule(response, rules)
	assert.Equal(&rules[0], matchingRule)
}

func TestHTTP_Rules_FullBodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rules := []Rule{
		{MatchingHTTPCodes: []int{500, 503}, MatchingBodyPart: "Invalid field 'attribute'"},
	}

	matchingRule := findMatchingRule(response, rules)
	assert.Equal(&rules[0], matchingRule)
}

func TestHTTP_Rules_PartialBodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rules := []Rule{
		{MatchingHTTPCodes: []int{500, 503}, MatchingBodyPart: "Invalid field"},
	}

	matchingRule := findMatchingRule(response, rules)
	assert.Equal(&rules[0], matchingRule)
}

func TestHTTP_Rules_StatusMatch_NoBodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rules := []Rule{
		{MatchingHTTPCodes: []int{500, 503}, MatchingBodyPart: "Invalid field 'events'"},
	}

	matchingRule := findMatchingRule(response, rules)
	assert.Nil(matchingRule)
}

func TestHTTP_Rules_NoStatusMatch_BodyMatch(t *testing.T) {
	assert := assert.New(t)

	response := response{Status: 500, Body: "Invalid field 'attribute'"}
	rules := []Rule{
		{MatchingHTTPCodes: []int{503}, MatchingBodyPart: "Invalid field"},
	}

	matchingRule := findMatchingRule(response, rules)
	assert.Nil(matchingRule)
}
