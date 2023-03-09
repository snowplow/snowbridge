// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package common

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
)

// --- Cloud Helpers

// GetGCPServiceAccountFromBase64 will take a base64 encoded string
// and attempt to create a JSON file on disk within the /tmp directory
// for later use in creating GCP clients.
func GetGCPServiceAccountFromBase64(serviceAccountB64 string) (string, error) {
	targetFile := fmt.Sprintf(`tmp_replicator/stream-replicator-service-account-%s.json`, uuid.NewV4().String())
	err := DecodeB64ToFile(serviceAccountB64, targetFile)
	if err != nil {
		return ``, err
	}
	return targetFile, nil
}

// DeleteTemporaryDir deletes the temp directory we created to store credentials
func DeleteTemporaryDir() error {
	err := os.RemoveAll(`tmp_replicator`)
	return err
}

// DecodeB64ToFile takes a B64-encoded credential, decodes it, and writes it to a file
func DecodeB64ToFile(b64String, filename string) error {
	tls, decodeErr := base64.StdEncoding.DecodeString(b64String)
	if decodeErr != nil {
		return errors.Wrap(decodeErr, "Failed to Base64 decode for creating file "+filename)
	}

	err := createTempDir(`tmp_replicator`)
	if err != nil {
		return err
	}

	f, createErr := os.Create(filename)
	if createErr != nil {
		return errors.Wrap(createErr, fmt.Sprintf("Failed to create file '%s'", filename))
	}

	_, writeErr := f.WriteString(string(tls))
	if writeErr != nil {
		return errors.Wrap(decodeErr, fmt.Sprintf("Failed to write decoded base64 string to target file '%s'", filename))
	}
	err = f.Close()
	if err != nil {
		return err
	}

	return nil
}

// GetAWSSession is a general tool to handle generating an AWS session
// using the standard auth flow.  We also have the ability to pass a role ARN
// to allow for roles to be assumed in cross-account access flows.
func GetAWSSession(region string, roleARN string) (sess *session.Session, cfg *aws.Config, accountID *string, err error) {
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		cfg = &aws.Config{
			Credentials: creds,
			Region:      aws.String(region),
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

// --- Generic Helpers

// GetAverageFromDuration will divide a duration by a total number and then return
// this value as another duration
func GetAverageFromDuration(sum time.Duration, total int64) time.Duration {
	if total > 0 {
		return time.Duration(int64(sum)/total) * time.Nanosecond
	}
	return time.Duration(0)
}

func createTempDir(dirName string) error {
	dir, statErr := os.Stat(dirName)
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return errors.Wrap(statErr, fmt.Sprintf("Failed checking for existence of %s dir", dirName))
	}

	if dir == nil {
		dirErr := os.Mkdir(dirName, 0700)
		if dirErr != nil && !errors.Is(dirErr, os.ErrExist) {
			return errors.Wrap(dirErr, fmt.Sprintf("Failed to create %s directory", dirName))
		}
	}
	return nil
}

// CreateTLSConfiguration creates a TLS configuration for use in a target
func CreateTLSConfiguration(crt, key, ca, destination string, skipVerify bool) (*tls.Config, error) {
	if crt == `` || key == `` || ca == `` {
		return nil, nil
	}
	tlsStrings := map[string]string{
		`.crt`:    crt,
		`.key`:    key,
		`_ca.crt`: ca,
	}
	// try to create /tmp_replicator/tls directory
	err := createTempDir(`tmp_replicator`)
	if err != nil {
		return nil, err
	}

	err = createTempDir(`tmp_replicator/tls`)
	if err != nil {
		return nil, err
	}

	// create destination directory
	err = os.Mkdir(`tmp_replicator/tls/`+destination, 0700)
	if err != nil {
		return nil, err
	}
	for key, tlsString := range tlsStrings {
		err := DecodeB64ToFile(tlsString, fmt.Sprintf(`tmp_replicator/tls/%s/%s%s`, destination, destination, key))
		if err != nil {
			return nil, err
		}
	}

	cert, err := tls.LoadX509KeyPair(
		fmt.Sprintf(`tmp_replicator/tls/%s/%s.crt`, destination, destination),
		fmt.Sprintf(`tmp_replicator/tls/%s/%s.key`, destination, destination))
	if err != nil {
		return nil, err
	}

	caCert, err := ioutil.ReadFile(fmt.Sprintf(`tmp_replicator/tls/%s/%s_ca.crt`, destination, destination))
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
