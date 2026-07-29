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

package http

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v6/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const jwtValidAccessToken = "jwt_bearer_access_token"
const jwtValidIssuer = "myservice@my-project.iam.gserviceaccount.com"
const jwtValidAudience = "https://mock.token.server/token"
const jwtBearerGrantType = "urn:ietf:params:oauth:grant-type:jwt-bearer"

// jwtTokenServer returns a mock token server that validates a JWT bearer assertion
// signed with the given RSA public key.
func jwtTokenServer(publicKey *rsa.PublicKey) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if err := req.ParseForm(); err != nil {
			logrus.Error(err.Error())
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		grantType := req.Form.Get("grant_type")
		assertion := req.Form.Get("assertion")

		if grantType != jwtBearerGrantType || assertion == "" {
			w.WriteHeader(http.StatusBadRequest)
			if _, err := fmt.Fprintf(w, `{"error":"invalid_request"}`); err != nil {
				logrus.Error(err.Error())
			}
			return
		}

		// Validate the JWT signature and claims.
		token, err := jwt.Parse(assertion, func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return publicKey, nil
		})
		if err != nil || !token.Valid {
			w.WriteHeader(http.StatusUnauthorized)
			if _, err2 := fmt.Fprintf(w, `{"error":"invalid_grant"}`); err2 != nil {
				logrus.Error(err2.Error())
			}
			return
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok || claims["iss"] != jwtValidIssuer {
			w.WriteHeader(http.StatusUnauthorized)
			if _, err := fmt.Fprintf(w, `{"error":"invalid_grant"}`); err != nil {
				logrus.Error(err.Error())
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if _, err := fmt.Fprintf(w, `{"access_token":"%s","expires_in":3600,"token_type":"Bearer"}`, jwtValidAccessToken); err != nil {
			logrus.Error(err.Error())
		}
	}))
}

// jwtTargetServer returns a mock target server that requires Bearer token auth.
func jwtTargetServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") == fmt.Sprintf("Bearer %s", jwtValidAccessToken) {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusForbidden)
			if _, err := fmt.Fprintf(w, "Invalid access token"); err != nil {
				logrus.Error(err.Error())
			}
		}
	}))
}

// generateRSAKeyPEM generates a fresh RSA key pair and returns the private key as a PEM byte slice.
func generateRSAKeyPEM(t *testing.T) ([]byte, *rsa.PublicKey) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	privPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	return privPEM, &privateKey.PublicKey
}

// writeKeyToTempFile writes PEM bytes to a temp file and returns the path.
func writeKeyToTempFile(t *testing.T, pemData []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "snowbridge-test-key-*.pem")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(f.Name()) })
	_, err = f.Write(pemData)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestHTTP_JWTAuth_Success(t *testing.T) {
	privPEM, pubKey := generateRSAKeyPEM(t)
	keyFile := writeKeyToTempFile(t, privPEM)

	tokenSrv := jwtTokenServer(pubKey)
	defer tokenSrv.Close()
	targetSrv := jwtTargetServer()
	defer targetSrv.Close()

	driver := &HTTPTargetDriver{}
	cfg := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	cfg.URL = targetSrv.URL
	cfg.OAuthJWT = &OAuthJWTConfig{
		KeyFile:  keyFile,
		TokenURL: tokenSrv.URL,
		Issuer:   jwtValidIssuer,
		Audience: jwtValidAudience,
	}

	require.NoError(t, driver.InitFromConfig(cfg))

	messages := testutil.GetTestMessages(1, `{"message": "hello"}`, func() {})
	result, err := driver.Write(messages)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result.Sent))
	assert.Equal(t, 0, len(result.Failed))
}

func TestHTTP_JWTAuth_InvalidKeyFile(t *testing.T) {
	driver := &HTTPTargetDriver{}
	cfg := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	cfg.URL = "http://localhost:9999"
	cfg.OAuthJWT = &OAuthJWTConfig{
		KeyFile:  "/nonexistent/path/to/key.pem",
		TokenURL: "http://localhost:9998/token",
		Issuer:   jwtValidIssuer,
	}

	err := driver.InitFromConfig(cfg)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "key_file"), "error should mention key_file")
}

func TestHTTP_JWTAuth_TokenServerRejects(t *testing.T) {
	// Token server that always rejects
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		if _, err := fmt.Fprintf(w, `{"error":"invalid_client"}`); err != nil {
			logrus.Error(err.Error())
		}
	}))
	defer tokenSrv.Close()

	targetSrv := jwtTargetServer()
	defer targetSrv.Close()

	privPEM, _ := generateRSAKeyPEM(t)
	keyFile := writeKeyToTempFile(t, privPEM)

	driver := &HTTPTargetDriver{}
	cfg := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	cfg.URL = targetSrv.URL
	cfg.OAuthJWT = &OAuthJWTConfig{
		KeyFile:  keyFile,
		TokenURL: tokenSrv.URL,
		Issuer:   jwtValidIssuer,
		Audience: jwtValidAudience,
	}

	require.NoError(t, driver.InitFromConfig(cfg))

	messages := testutil.GetTestMessages(1, `{"message": "hello"}`, func() {})
	result, err := driver.Write(messages)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Client failed to complete request")
	assert.Equal(t, 0, len(result.Sent))
	assert.Equal(t, 1, len(result.Failed))
}

// TestHTTP_JWTAuth_InlineKey_Success tests the jwt_auth_key (inline PEM string) path end-to-end.
func TestHTTP_JWTAuth_InlineKey_Success(t *testing.T) {
	privPEM, pubKey := generateRSAKeyPEM(t)

	tokenSrv := jwtTokenServer(pubKey)
	defer tokenSrv.Close()
	targetSrv := jwtTargetServer()
	defer targetSrv.Close()

	driver := &HTTPTargetDriver{}
	cfg := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	cfg.URL = targetSrv.URL
	cfg.OAuthJWT = &OAuthJWTConfig{
		Key:      string(privPEM), // inline PEM string, not a file path
		TokenURL: tokenSrv.URL,
		Issuer:   jwtValidIssuer,
		Audience: jwtValidAudience,
	}

	require.NoError(t, driver.InitFromConfig(cfg))

	messages := testutil.GetTestMessages(1, `{"message": "hello"}`, func() {})
	result, err := driver.Write(messages)

	assert.NoError(t, err)
	assert.Equal(t, 1, len(result.Sent))
	assert.Equal(t, 0, len(result.Failed))
}

func TestHTTP_JWTAuth_KeyAndKeyFileMutuallyExclusive(t *testing.T) {
	driver := &HTTPTargetDriver{}
	cfg := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	cfg.URL = "http://localhost:9999"
	cfg.OAuthJWT = &OAuthJWTConfig{
		Key:      "some-inline-pem",
		KeyFile:  "/some/key.pem",
		TokenURL: "http://localhost:9998/token",
		Issuer:   "some-issuer",
	}

	err := driver.InitFromConfig(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestHTTP_OAuthClientAndJWT_MutuallyExclusive(t *testing.T) {
	driver := &HTTPTargetDriver{}
	cfg := driver.GetDefaultConfiguration().(*HTTPTargetConfig)
	cfg.URL = "http://localhost:9999"
	cfg.OAuthClient = &OAuthClientConfig{
		ClientID: "some-client-id",
		TokenURL: "http://localhost:9998/token",
	}
	cfg.OAuthJWT = &OAuthJWTConfig{
		KeyFile:  "/some/key.pem",
		TokenURL: "http://localhost:9998/token",
		Issuer:   "some-issuer",
	}

	err := driver.InitFromConfig(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}
