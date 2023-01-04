//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package sourceconfig

import (
	"fmt"
	"strings"

	config "github.com/snowplow/snowbridge/config"
	"github.com/snowplow/snowbridge/pkg/source/sourceiface"
)

// GetSource creates and returns the source that is configured.
func GetSource(c *config.Config, supportedSources []config.ConfigurationPair) (sourceiface.Source, error) {
	useSource := c.Data.Source.Use
	decoderOpts := &config.DecoderOptions{
		Input: useSource.Body,
	}

	sourceList := make([]string, 0)
	for _, pair := range supportedSources {
		if pair.Name == useSource.Name {
			plug := pair.Handle
			component, err := c.CreateComponent(plug, decoderOpts)
			if err != nil {
				return nil, err
			}

			if s, ok := component.(sourceiface.Source); ok {
				return s, nil
			}

			return nil, fmt.Errorf("could not interpret source configuration for %q", useSource.Name)
		}
		sourceList = append(sourceList, pair.Name)
	}
	return nil, fmt.Errorf("Invalid source found: %s. Supported sources in this build: %s", useSource.Name, strings.Join(sourceList, ", "))
}
