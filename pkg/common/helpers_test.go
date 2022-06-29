// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package common

import (
	"bytes"
	"encoding/base64"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func init() {
	os.Clearenv()
}

// --- Cloud Helpers

func TestGetGCPServiceAccountFromBase64(t *testing.T) {
	assert := assert.New(t)
	defer DeleteTemporaryDir()

	path, err := GetGCPServiceAccountFromBase64("ewogICJoZWxsbyI6IndvcmxkIgp9")

	assert.NotEqual(path, "")
	assert.Nil(err)
	assert.True(strings.HasPrefix(path, "tmp_replicator/stream-replicator-service-account-"))
	assert.True(strings.HasSuffix(path, ".json"))
}

func TestGetGCPServiceAccountFromBase64_NotBase64(t *testing.T) {
	assert := assert.New(t)

	path, err := GetGCPServiceAccountFromBase64("helloworld")

	assert.Equal("", path)
	assert.NotNil(err)
	assert.True(strings.HasPrefix(err.Error(), "Failed to Base64 decode"))
}

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
	crt, err := os.ReadFile(`../../integration/http/localhost.crt`)
	if err != nil {
		return
	}
	encodedCrt := base64.StdEncoding.EncodeToString(crt)
	key, err := os.ReadFile(`../../integration/http/localhost.key`)
	if err != nil {
		return
	}
	encodedKey := base64.StdEncoding.EncodeToString(key)
	ca, err := os.ReadFile(`../../integration/http/rootCA.crt`)
	if err != nil {
		return
	}
	encodedCa := base64.StdEncoding.EncodeToString(ca)
	_, err = CreateTLSConfiguration(encodedCrt, encodedKey, encodedCa, `kafka`, false)
	files, readErr := ioutil.ReadDir("./tmp_replicator/tls/kafka")
	if readErr != nil {
		return
	}

	assert.Nil(err)
	assert.Equal(3, len(files))
	assert.Equal(`kafka.crt`, files[0].Name())
	f, err := os.ReadFile(`tmp_replicator/tls/kafka/kafka.crt`)

	assert.Nil(err)
	assert.True(bytes.Equal(f, crt))

	f, err = os.ReadFile(`tmp_replicator/tls/kafka/kafka.key`)

	assert.Nil(err)
	assert.True(bytes.Equal(f, key))

	f, err = os.ReadFile(`tmp_replicator/tls/kafka/kafka_ca.crt`)

	assert.Nil(err)
	assert.True(bytes.Equal(f, ca))
	os.RemoveAll(`tmp_replicator`)
}

func TestCreateTLSConfiguration_DirExists(t *testing.T) {
	os.MkdirAll(`tmp_replicator/tls/kafka`, 0777)
	assert := assert.New(t)
	_, err := CreateTLSConfiguration("dGVzdA==", "dGVzdA==", "dGVzdA==", `kafka`, false)
	files, readErr := ioutil.ReadDir("./tmp_replicator/tls/kafka")
	if readErr != nil {
		return
	}

	assert.Error(err)
	assert.Equal(0, len(files))

	os.RemoveAll(`tmp_replicator`)
}

func TestCreateTLSConfiguration_NotB64(t *testing.T) {
	assert := assert.New(t)
	_, err := CreateTLSConfiguration("helloworld", "helloworld", "helloworld", `kafka`, false)
	files, readErr := ioutil.ReadDir("./tmp_replicator/tls/kafka")
	if readErr != nil {
		return
	}

	assert.True(strings.HasPrefix(err.Error(), `Failed to Base64 decode for creating file `))
	assert.Equal(0, len(files))

	os.RemoveAll(`tmp_replicator`)
}
