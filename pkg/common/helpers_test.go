// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package common

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --- Cloud Helpers

func TestGetGCPServiceAccountFromBase64(t *testing.T) {
	assert := assert.New(t)

	path, err := GetGCPServiceAccountFromBase64("ewogICJoZWxsbyI6IndvcmxkIgp9")

	assert.NotEqual(path, "")
	assert.Nil(err)
	assert.True(strings.HasPrefix(path, "/tmp/stream-replicator-service-account-"))
	assert.True(strings.HasSuffix(path, ".json"))
}

func TestGetGCPServiceAccountFromBase64_NotBase64(t *testing.T) {
	assert := assert.New(t)

	path, err := GetGCPServiceAccountFromBase64("helloworld")

	assert.Equal(path, "")
	assert.NotNil(err)
	assert.True(strings.HasPrefix(err.Error(), "Failed to Base64 decode service account: "))
}

func TestGetAWSSession(t *testing.T) {
	assert := assert.New(t)

	sess, cfg, accID, err := GetAWSSession("us-east-1", "")
	assert.NotNil(sess)
	assert.Nil(cfg)
	assert.Nil(accID)
	assert.NotNil(err)

	sess2, cfg2, accID2, err2 := GetAWSSession("us-east-1", "some-role-arn")
	assert.NotNil(sess2)
	assert.NotNil(cfg2)
	assert.Nil(accID2)
	assert.NotNil(err2)
}

// --- Generic Helpers

func TestGetAverageFromDuration(t *testing.T) {
	assert := assert.New(t)

	duration := GetAverageFromDuration(time.Duration(0), 0)
	assert.Equal(time.Duration(0), duration)

	duration2 := GetAverageFromDuration(time.Duration(10)*time.Second, 2)
	assert.Equal(time.Duration(5)*time.Second, duration2)
}
