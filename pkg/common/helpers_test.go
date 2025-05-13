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
	sess, cfg, accID, err := GetAWSSession("us-east-1", "", "")
	assert.NotNil(sess)
	assert.Nil(cfg)
	assert.Nil(accID)
	assert.NotNil(err)

	sess2, cfg2, accID2, err2 := GetAWSSession("us-east-1", "some-role-arn", "")
	assert.NotNil(sess2)
	assert.NotNil(cfg2)
	assert.Nil(accID2)
	assert.NotNil(err2)
	if err2 != nil {
		assert.Equal("InvalidParameter: 1 validation error(s) found.\n- minimum field size of 20, AssumeRoleInput.RoleArn.\n", err2.Error())
	}
}

func TestGetAWSConfig(t *testing.T) {
	assert := assert.New(t)

	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "")

	cfg, accID, err := GetAWSConfig("us-east-1", "", "")
	assert.NotNil(cfg)
	assert.Equal("", accID)
	assert.NotNil(err)

	cfg2, accID2, err2 := GetAWSConfig("us-east-1", "some-role-arn", "")
	assert.NotNil(cfg2)
	assert.Equal("", accID2)
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

func TestCreateTLSConfiguration(t *testing.T) {
	assert := assert.New(t)

	conf, err := CreateTLSConfiguration(`../../integration/http/localhost.crt`, `../../integration/http/localhost.key`, `../../integration/http/rootCA.crt`, false)

	assert.Nil(err)
	assert.IsType(tls.Config{}, *conf.Clone())
}
