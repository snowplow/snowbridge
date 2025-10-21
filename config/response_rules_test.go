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

package config

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/snowplow/snowbridge/v3/pkg/models"
	"github.com/snowplow/snowbridge/v3/pkg/target"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResponseRules(t *testing.T) {
	rules := `
		response_rules {
			setup {
				http_codes = [400]
				body = "Invalid API key"
			}
			invalid {
				http_codes = [400]
			}
		}`

	result, err := run(rules, 400, "Invalid API key")

	// This is 1
	fmt.Printf("invalid len: %d\n", len(result.Invalid))

	_, ok := err.(models.SetupWriteError)
	//This is false
	fmt.Printf("is setup: %v\n", ok)
}

func run(rules string, returnedCode int, returnedBody string) (*models.TargetWriteResult, error) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(returnedCode)
		if _, err := w.Write([]byte(returnedBody)); err != nil {
			logrus.Error(err.Error())
		}
	}))
	defer server.Close()

	hclConfig := `
target {
  use "http" {
    url = "` + server.URL + `"
   ` + rules + `
  }
}
`

	cfg, err := NewHclConfig([]byte(hclConfig), "test.hcl")
	plug := target.AdaptHTTPTargetFunc(target.HTTPTargetConfigFunction)
	decoderOpts := &DecoderOptions{
		Input: cfg.Data.Target.Use.Body,
	}

	component, err := cfg.CreateComponent(plug, decoderOpts)
	target, _ := component.(*target.HTTPTarget)

	input := []*models.Message{{Data: []byte(`{"user": "test"}`)}}
	writeResult, err := target.Write(input)
	return writeResult, err
}
