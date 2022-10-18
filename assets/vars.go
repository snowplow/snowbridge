// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

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
