// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestStoreGCPServiceAccountFromBase64(t *testing.T) {
	assert := assert.New(t)

	path, err := storeGCPServiceAccountFromBase64("ewogICJoZWxsbyI6IndvcmxkIgp9")

	assert.NotEqual(path, "")
	assert.Nil(err)
	assert.True(strings.HasPrefix(path, "/tmp/stream-replicator-service-account-"))
	assert.True(strings.HasSuffix(path, ".json"))
}

func TestStoreGCPServiceAccountFromBase64_NotBase64(t *testing.T) {
	assert := assert.New(t)

	path, err := storeGCPServiceAccountFromBase64("helloworld")

	assert.Equal(path, "")
	assert.NotNil(err)
	assert.True(strings.HasPrefix(err.Error(), "Could not Base64 decode service account: "))
}
