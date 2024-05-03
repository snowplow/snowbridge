/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

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
