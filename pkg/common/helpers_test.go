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
	"crypto/x509"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/snowplow/snowbridge/v6/pkg/testutil/tlstest"
)

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
	files := tlstest.GenerateFiles(t)

	conf, err := CreateTLSConfiguration(files.ClientCert, files.ClientKey, files.CACert, false)
	require.NoError(t, err)
	require.NotNil(t, conf)

	clientCert, err := tls.LoadX509KeyPair(files.ClientCert, files.ClientKey)
	require.NoError(t, err)
	assert.Equal([]tls.Certificate{clientCert}, conf.Certificates)
	assert.False(conf.InsecureSkipVerify)

	caPEM, err := os.ReadFile(files.CACert)
	require.NoError(t, err)
	expectedRoots, err := x509.SystemCertPool()
	require.NoError(t, err)
	require.True(t, expectedRoots.AppendCertsFromPEM(caPEM))
	assert.True(expectedRoots.Equal(conf.RootCAs), "RootCAs should be the system pool plus the CA from ca_file")

	caOnly := x509.NewCertPool()
	caOnly.AppendCertsFromPEM(caPEM)
	assert.False(caOnly.Equal(conf.RootCAs), "RootCAs should not be built from an empty pool")
}
