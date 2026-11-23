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

package cli

import (
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/snowplow/snowbridge/v6/config"
	"github.com/snowplow/snowbridge/v6/pkg/target/http"
	"github.com/snowplow/snowbridge/v6/pkg/target/targetconfig"
)

// connectionCheckOutput is the JSON structure shared with other connection tests
type connectionCheckOutput struct {
	ID        string              `json:"id"`
	Name      string              `json:"name"`
	Version   int                 `json:"version"`
	EmittedBy string              `json:"emittedBy"`
	Timestamp string              `json:"timestamp"`
	Data      connectionCheckData `json:"data"`
}

type connectionCheckData struct {
	Host     string            `json:"host"`
	Complete bool              `json:"complete"`
	Messages []string          `json:"messages"`
	Tags     map[string]string `json:"tags"`
	Attempts int               `json:"attempts"`
}

// RunConnectionTest builds the main target, runs its connection test once, and writes the result
// as JSON to stdout. Whether the check passed is reported in the JSON, not by returning an error -
// an error is returned only when the check could not be run at all.
func RunConnectionTest(cfg *config.Config) error {
	target, err := targetconfig.GetTarget(cfg.Data.Target, cfg.Decoder)
	if err != nil {
		return err
	}

	httpTarget, ok := target.TargetDriver.(*http.HTTPTargetDriver)
	if !ok {
		return errors.New("connection test is only supported for the http target")
	}

	result, err := httpTarget.CheckConnection()
	if err != nil {
		return err
	}

	output := connectionCheckOutput{
		ID:        uuid.NewString(),
		Name:      "snowbridge:http-target-connection-check",
		Version:   1,
		EmittedBy: "snowbridge",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data: connectionCheckData{
			Host:     result.Host,
			Complete: result.Complete,
			Messages: result.Messages,
			Tags:     map[string]string{},
			Attempts: 1,
		},
	}

	return json.NewEncoder(os.Stdout).Encode(output)
}
