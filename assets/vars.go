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
