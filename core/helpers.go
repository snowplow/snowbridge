// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/twinj/uuid"
	"os"
)

// --- Cloud Helpers

func storeGCPServiceAccountFromBase64(serviceAccountB64 string) (string, error) {
	sDec, err := base64.StdEncoding.DecodeString(serviceAccountB64)
	if err != nil {
		return "", fmt.Errorf("Could not Base64 decode service account: %s", err.Error())
	}

	targetFile := fmt.Sprintf("/tmp/stream-replicator-service-account-%s.json", uuid.NewV4().String())

	f, err := os.Create(targetFile)
	if err != nil {
		return "", fmt.Errorf("Could not create target file '%s' for service account: %s", targetFile, err.Error())
	}
	defer f.Close()

	_, err2 := f.WriteString(string(sDec))
	if err2 != nil {
		return "", fmt.Errorf("Could not write decoded service account to target file '%s': %s", targetFile, err.Error())
	}

	return targetFile, nil
}

func getAWSSession(region string, roleARN string) (*session.Session, *aws.Config) {
	session := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	if roleARN != "" {
		creds := stscreds.NewCredentials(session, roleARN)
		config := aws.Config{
			Credentials: creds,
			Region:      aws.String(region),
		}

		return session, &config
	}
	return session, nil
}

// --- Generic Helpers

// getChunkedEvents returns an array of chunked arrays from the original slice
func getChunkedEvents(events []*Event, chunkSize int) [][]*Event {
	var divided [][]*Event
	for i := 0; i < len(events); i += chunkSize {
		end := i + chunkSize
		if end > len(events) {
			end = len(events)
		}
		divided = append(divided, events[i:end])
	}
	return divided
}
