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
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	stscredsv2 "github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	stsv2 "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/xdg/scram"
)

// GetAWSSession is a general tool to handle generating an AWS session
// using the standard auth flow.  We also have the ability to pass a role ARN
// to allow for roles to be assumed in cross-account access flows.
func GetAWSSession(region string, roleARN string, endpoint string) (sess *session.Session, cfg *aws.Config, accountID *string, err error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns
	httpClient := &http.Client{
		Transport: transport,
	}

	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region:     aws.String(region),
			Endpoint:   aws.String(endpoint),
			HTTPClient: httpClient,
		},
	}))

	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		cfg = &aws.Config{
			Credentials: creds,
			Region:      aws.String(region),
			HTTPClient:  httpClient,
		}
	}

	stsClient := sts.New(sess, cfg)

	res, err := stsClient.GetCallerIdentity(&sts.GetCallerIdentityInput{})
	if err != nil {
		return sess, cfg, nil, err
	}
	accountID = res.Account

	return sess, cfg, accountID, nil
}

// GetAWSConfig is a general tool to handle generating an AWS config
// using the standard auth flow.
// We also have the ability to pass a role ARN to allow for roles
// to be assumed in cross-account access flows.
func GetAWSConfig(region, roleARN, endpoint string) (*awsv2.Config, string, error) {
	ctx := context.Background()

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = transport.MaxIdleConns
	httpClient := &http.Client{
		Transport: transport,
	}

	conf, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, "", err
	}

	stsClient := stsv2.NewFromConfig(conf, func(o *stsv2.Options) {
		o.BaseEndpoint = &endpoint
	})

	if roleARN != "" {
		creds := stscredsv2.NewAssumeRoleProvider(stsClient, roleARN)
		conf, err = config.LoadDefaultConfig(
			ctx,
			config.WithCredentialsProvider(creds),
			config.WithRegion(region),
			config.WithHTTPClient(httpClient),
		)
		if err != nil {
			return nil, "", err
		}
	}

	res, err := stsClient.GetCallerIdentity(ctx, &stsv2.GetCallerIdentityInput{})
	if err != nil {
		return &conf, "", err
	}

	accountID := *res.Account
	return &conf, accountID, nil
}

// --- Generic Helpers

// GetAverageFromDuration will divide a duration by a total number and then return
// this value as another duration
func GetAverageFromDuration(sum time.Duration, total int64) time.Duration {
	if total > 0 {
		return time.Duration(int64(sum)/total) * time.Nanosecond
	}
	return time.Duration(0)
}

// CreateTLSConfiguration creates a TLS configuration for use in a target
func CreateTLSConfiguration(certFile string, keyFile string, caFile string, skipVerify bool) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, nil
	}

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caCertPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: skipVerify,
	}, nil
}

// GetKafkaVersion checks the providede version against supported kafka versions and returns a sarama version
func GetKafkaVersion(targetVersion string) (sarama.KafkaVersion, error) {
	preferredVersion := sarama.DefaultVersion

	if targetVersion != "" {
		parsedVersion, err := sarama.ParseKafkaVersion(targetVersion)
		if err != nil {
			return sarama.DefaultVersion, err
		}

		supportedVersion := false
		for _, version := range sarama.SupportedVersions {
			if version == parsedVersion {
				supportedVersion = true
				preferredVersion = parsedVersion
				break
			}
		}
		if !supportedVersion {
			return sarama.DefaultVersion, fmt.Errorf("unsupported version `%s`. select older, compatible version instead", parsedVersion)
		}
	}

	return preferredVersion, nil
}

// ConfigureSASL returns an SASL config
func ConfigureSASL(saslAlgo, saslUser, saslPassword string) (SASL, error) {
	sasl := SASL{
		Enable:    true,
		User:      saslUser,
		Password:  saslPassword,
		Handshake: true,
	}

	switch saslAlgo {
	case "sha512":
		sasl.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{HashGeneratorFcn: SHA512}
		}
		sasl.Mechanism = sarama.SASLTypeSCRAMSHA512
	case "sha256":
		sasl.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: SHA256} }
		sasl.Mechanism = sarama.SASLTypeSCRAMSHA256
	case "plaintext":
		sasl.Mechanism = sarama.SASLTypePlaintext
	default:
		return SASL{}, fmt.Errorf("invalid SHA algorithm \"%s\": can be either \"sha256\" or"+
			" \"sha512\"",
			saslAlgo)
	}

	return sasl, nil
}

// SHA256 hash
var SHA256 scram.HashGeneratorFcn = func() hash.Hash { return sha256.New() }

// SHA512 hash
var SHA512 scram.HashGeneratorFcn = func() hash.Hash { return sha512.New() }

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// SASL based authentication with broker. While there are multiple SASL authentication methods
// the current implementation is limited to plaintext (SASL/PLAIN) authentication
// The nested SASL is extracted from sarama.Config.Net.SASL.
type SASL struct {
	// Whether or not to use SASL authentication when connecting to the broker
	// (defaults to false).
	Enable bool
	// SASLMechanism is the name of the enabled SASL mechanism.
	// Possible values: OAUTHBEARER, PLAIN (defaults to PLAIN).
	Mechanism sarama.SASLMechanism
	// Version is the SASL Protocol Version to use
	// Kafka > 1.x should use V1, except on Azure EventHub which use V0
	Version int16
	// Whether or not to send the Kafka SASL handshake first if enabled
	// (defaults to true). You should only set this to false if you're using
	// a non-Kafka SASL proxy.
	Handshake bool
	// AuthIdentity is an (optional) authorization identity (authzid) to
	// use for SASL/PLAIN authentication (if different from User) when
	// an authenticated user is permitted to act as the presented
	// alternative user. See RFC4616 for details.
	AuthIdentity string
	// User is the authentication identity (authcid) to present for
	// SASL/PLAIN or SASL/SCRAM authentication
	User string
	// Password for SASL/PLAIN authentication
	Password string
	// authz id used for SASL/SCRAM authentication
	SCRAMAuthzID string
	// SCRAMClientGeneratorFunc is a generator of a user provided implementation of a SCRAM
	// client used to perform the SCRAM exchange with the server.
	SCRAMClientGeneratorFunc func() sarama.SCRAMClient
	// TokenProvider is a user-defined callback for generating
	// access tokens for SASL/OAUTHBEARER auth. See the
	// AccessTokenProvider interface docs for proper implementation
	// guidelines.
	TokenProvider sarama.AccessTokenProvider

	GSSAPI sarama.GSSAPIConfig
}
