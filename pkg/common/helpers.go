// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package common

import (
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/pkg/errors"
	"github.com/twinj/uuid"
	"os"
	"time"
)

// --- Cloud Helpers

// GetGCPServiceAccountFromBase64 will take a base64 encoded string
// and attempt to create a JSON file on disk within the /tmp directory
// for later use in creating GCP clients.
func GetGCPServiceAccountFromBase64(serviceAccountB64 string) (string, error) {
	sDec, err := base64.StdEncoding.DecodeString(serviceAccountB64)
	if err != nil {
		return "", errors.Wrap(err, "Failed to Base64 decode service account")
	}

	targetFile := fmt.Sprintf("/tmp/stream-replicator-service-account-%s.json", uuid.NewV4().String())

	f, err := os.Create(targetFile)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to create target file '%s' for service account", targetFile))
	}
	defer f.Close()

	_, err2 := f.WriteString(string(sDec))
	if err2 != nil {
		return "", errors.Wrap(err, fmt.Sprintf("Failed to write decoded service account to target file '%s'", targetFile))
	}

	return targetFile, nil
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
