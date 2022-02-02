// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package sourceconfig

import (
	"errors"
	"fmt"
	"strings"

	config "github.com/snowplow-devops/stream-replicator/config/common"
	"github.com/snowplow-devops/stream-replicator/pkg/source/sourceiface"
)

// SourceConfigFunction is a function which returns a source.
type SourceConfigFunction func(*config.Config) (sourceiface.Source, error)

// SourceConfigPair contains the name of a source and its sourceConfigFunction.
type SourceConfigPair struct {
	SourceName       string
	SourceConfigFunc SourceConfigFunction
}

func GetSource(c *config.Config, supportedSources []SourceConfigPair) (sourceiface.Source, error) {
	sourceList := make([]string, 0)
	for _, configPair := range supportedSources {
		if configPair.SourceName == c.Source {
			return configPair.SourceConfigFunc(c)
		}
		sourceList = append(sourceList, configPair.SourceName)
	}
	return nil, errors.New(fmt.Sprintf("Invalid source found: %s. Supported sources in this build: %s.", c.Source, strings.Join(sourceList, ", ")))
}
