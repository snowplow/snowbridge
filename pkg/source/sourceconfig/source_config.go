// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package sourceconfig

import (
	"fmt"
	"strings"

	config "github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
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
