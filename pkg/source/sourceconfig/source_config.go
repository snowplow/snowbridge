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

// ConfigFunction is a function which returns a source.
type ConfigFunction func(*config.Config) (sourceiface.Source, error)

// ConfigPair contains the name of a source and its ConfigFunction.
type ConfigPair struct {
	SourceName       string
	SourceConfigFunc ConfigFunction
}

// GetSource iterates the list of supported sources, matches the provided config for source, and returns a source.
func GetSource(c *config.Config, supportedSources []ConfigPair) (sourceiface.Source, error) {
	sourceList := make([]string, 0)
	for _, configPair := range supportedSources {
		if configPair.SourceName == c.Source {
			return configPair.SourceConfigFunc(c)
		}
		sourceList = append(sourceList, configPair.SourceName)
	}
	return nil, fmt.Errorf("Invalid source found: %s. Supported sources in this build: %s", c.Source, strings.Join(sourceList, ", "))
}
