// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package core

import (
	"encoding/base64"
	"fmt"
	"github.com/twinj/uuid"
	"os"
)

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
