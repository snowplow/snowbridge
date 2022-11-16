// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package docs

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snowplow/snowbridge/assets"
	"github.com/snowplow/snowbridge/config"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	os.Clearenv()

	exitVal := m.Run()

	os.Exit(exitVal)
}

var jsScriptPath = filepath.Join(assets.AssetsRootDir, "docs", "configuration", "transformations", "custom-scripts", "create-a-script-filter-example.js")

func checkComponentForZeros(t *testing.T, component interface{}) {
	assert := assert.New(t)

	// Indirect dereferences the pointer for us
	valOfComponent := reflect.Indirect(reflect.ValueOf(component))
	typeOfComponent := valOfComponent.Type()

	var zerosFound []string

	for i := 0; i < typeOfComponent.NumField(); i++ {
		if valOfComponent.Field(i).IsZero() {
			zerosFound = append(zerosFound, typeOfComponent.Field(i).Name)
		}
	}

	// Check for empty fields in example config
	assert.Equal(0, len(zerosFound), fmt.Sprintf("Example config for %v -results in zero values for : %v - either fields are missing in the example, or are set to zero value", typeOfComponent, zerosFound))
}

func getConfigFromFilepath(t *testing.T, filepath string) *config.Config {
	assert := assert.New(t)
	t.Setenv("SNOWBRIDGE_CONFIG_FILE", filepath)
	c, newConfErr := config.NewConfig()
	assert.NotNil(c)
	assert.Nil(newConfErr)
	if newConfErr != nil {
		assert.Fail(newConfErr.Error())
	}

	return c
}
