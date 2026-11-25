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

package tlstest

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateFiles(t *testing.T) {
	assert := assert.New(t)
	files := GenerateFiles(t)

	caPEM, err := os.ReadFile(files.CACert)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))

	server, err := tls.LoadX509KeyPair(files.ServerCert, files.ServerKey)
	require.NoError(t, err)
	_, err = server.Leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: "localhost", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}})
	assert.NoError(err)
	assert.NoError(server.Leaf.VerifyHostname("127.0.0.1"))

	client, err := tls.LoadX509KeyPair(files.ClientCert, files.ClientKey)
	require.NoError(t, err)
	_, err = client.Leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	assert.NoError(err)
}

func TestGenerateFiles_EachCallCreatesANewCA(t *testing.T) {
	files := GenerateFiles(t)
	other := GenerateFiles(t)

	caPEM, err := os.ReadFile(files.CACert)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(caPEM))

	otherClient, err := tls.LoadX509KeyPair(other.ClientCert, other.ClientKey)
	require.NoError(t, err)
	_, err = otherClient.Leaf.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	assert.Error(t, err)
}
