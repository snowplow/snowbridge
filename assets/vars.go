//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package assets

import (
	"runtime"
	"strings"
)

// GetPathToAssetsDir is a utility function which returns the absolute path to the
// assets directory which houses this file. Its purpose is to facilitate housing
// test configurations in one place and reusing them throughout the project.
func GetPathToAssetsDir() string {
	_, filename, _, _ := runtime.Caller(0)

	parts := strings.Split(filename, "/")
	dirPath := strings.Join(parts[:len(parts)-1], "/")
	return dirPath
}

// AssetsRootDir is the absolute path to `assets/`
var AssetsRootDir = GetPathToAssetsDir()
