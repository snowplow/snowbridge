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
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/testutil"
	"github.com/stretchr/testify/assert"
)

// that's what we configure in our target
const validClientID = "CLIENT_ID_TEST"
const validClientSecret = "CLIENT_SECRET_TEST"
const validRefreshToken = "REFRESH_TOKEN_TEST"
const validGrantType = "refresh_token"

// that's what is returned by mock token server and used as bearer token to authorize request to target server
const validAccessToken = "super_secret_access_token"

// This is mock server providing us the bearer access token. If you provide invalid details/something is misconfigured you get 400 HTTP status
func tokenServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := req.ParseForm(); err != nil {
			logrus.Error(err.Error())
		}
		clientID, clientSecret, _ := req.BasicAuth()
		refreshToken := req.Form.Get("refresh_token")
		grantType := req.Form.Get("grant_type")

		if clientID == validClientID && clientSecret == validClientSecret && refreshToken == validRefreshToken && grantType == validGrantType {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			if _, err := fmt.Fprintf(w, `{"access_token":"%s", "expires_in":3600}`, validAccessToken); err != nil {
				logrus.Error(err.Error())
			}
		} else {
			w.WriteHeader(400)
			if _, err := fmt.Fprintf(w, `{"error":"invalid_client"}`); err != nil {
				logrus.Error(err.Error())
			}
		}
	}))
}

// This is mock target server which requires us to provide valid access token. Without valid token you set 403 HTTP status
func targetServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") == fmt.Sprintf("Bearer %s", validAccessToken) {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(403)
			if _, err := fmt.Fprintf(w, "Invalid access token"); err != nil {
				logrus.Error(err.Error())
			}
		}
	}))
}

func TestHTTP_OAuth2_Success(t *testing.T) {
	assert := assert.New(t)

	writeResult, err := runTest(t, validClientID, validClientSecret, validRefreshToken)

	assert.Nil(err)
	assert.Equal(1, len(writeResult.Sent))
	assert.Equal(0, len(writeResult.Failed))
}

func TestHTTP_OAuth2_CanNotFetchToken(t *testing.T) {
	testCases := []struct {
		Name              string
		InputClientID     string
		InputClientSecret string
		InputRefreshToken string
	}{
		{Name: "Invalid client id", InputClientID: "INVALID", InputClientSecret: validClientSecret, InputRefreshToken: validRefreshToken},
		{Name: "Invalid client secret", InputClientID: validClientID, InputClientSecret: "INVALID", InputRefreshToken: validRefreshToken},
		{Name: "Invalid refresh token", InputClientID: validClientID, InputClientSecret: validClientSecret, InputRefreshToken: "INVALID"},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			writeResult, err := runTest(t, tt.InputClientID, tt.InputClientSecret, tt.InputRefreshToken)

			assert.NotNil(err)
			assert.Contains(err.Error(), `Client failed to complete request`)
			assert.Equal(0, len(writeResult.Sent))
			assert.Equal(1, len(writeResult.Failed))
			assert.Contains(writeResult.Failed[0].GetError().Error(), `{"error":"invalid_client"}`)
		})
	}
}

func TestHTTP_OAuth2_CallTargetWithoutToken(t *testing.T) {
	assert := assert.New(t)
	writeResult, err := runTest(t, "", "", "")

	assert.NotNil(err)
	assert.Contains(err.Error(), `got transient error, response status: '403 Forbidden'`)
	assert.Equal(0, len(writeResult.Sent))
	assert.Equal(1, len(writeResult.Failed))
}

func runTest(t *testing.T, inputClientID string, inputClientSecret string, inputRefreshToken string) (*models.TargetWriteResult, error) {
	tokenServer := tokenServer()
	server := targetServer()
	defer tokenServer.Close()
	defer server.Close()

	config := defaultConfiguration()
	config.URL = server.URL
	config.OAuth2ClientID = inputClientID
	config.OAuth2ClientSecret = inputClientSecret
	config.OAuth2RefreshToken = inputRefreshToken
	config.OAuth2TokenURL = tokenServer.URL
	target, err := HTTPTargetConfigFunction(config)

	if err != nil {
		t.Fatal(err)
	}

	message := testutil.GetTestMessages(1, `{"message": "Hello Server!!"}`, func() {})
	return target.Write(message)
}
