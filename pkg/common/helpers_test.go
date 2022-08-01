// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package common

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetAWSSession(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "")
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
	if err != nil {
		assert.Equal("InvalidParameter: 1 validation error(s) found.\n- minimum field size of 20, AssumeRoleInput.RoleArn.\n", err2.Error())
	}
}

// --- Generic Helpers

func TestGetAverageFromDuration(t *testing.T) {
	assert := assert.New(t)

	duration := GetAverageFromDuration(time.Duration(0), 0)
	assert.Equal(time.Duration(0), duration)

	duration2 := GetAverageFromDuration(time.Duration(10)*time.Second, 2)
	assert.Equal(time.Duration(5)*time.Second, duration2)
}

func TestCreateTLSConfiguration(t *testing.T) {
	assert := assert.New(t)

	conf, err := CreateTLSConfiguration(`../../integration/http/localhost.crt`, `../../integration/http/localhost.key`, `../../integration/http/rootCA.crt`, false)

	assert.Nil(err)
	assert.IsType(tls.Config{}, *conf)
}
