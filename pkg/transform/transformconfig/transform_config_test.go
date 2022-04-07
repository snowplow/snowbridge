// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package transformconfig

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/snowplow-devops/stream-replicator/config"
	"github.com/snowplow-devops/stream-replicator/pkg/models"
	"github.com/snowplow-devops/stream-replicator/pkg/transform"
)

func TestParseTransformations_InvalidMessage(t *testing.T) {
	testCases := []struct {
		Name     string
		Message  string
		ExpError string
	}{
		{
			Name:     "message_empty",
			Message:  "",
			ExpError: "invalid message transformation found; empty string",
		},
		{
			Name:     "message_not_found",
			Message:  "fake",
			ExpError: "invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk', 'spEnrichedFilter', 'spEnrichedFilterContext', 'spEnrichedFilterUnstructEvent', 'lua', 'js' or 'none' but got \"fake\"",
		},
		{
			Name:     "message_option_none_a",
			Message:  "none:wrong",
			ExpError: "invalid message transformation found; unexpected colon after \"none\"",
		},
		{
			Name:     "message_option_none_b",
			Message:  "none:",
			ExpError: "invalid message transformation found; unexpected colon after \"none\"",
		},
		{
			Name:     "message_option_spEnrichedToJson",
			Message:  "spEnrichedToJson:wrong",
			ExpError: "invalid message transformation found; unexpected colon after \"spEnrichedToJson\"",
		},
		{
			Name:     "message_no_option_spEnrichedSetPk",
			Message:  "spEnrichedSetPk",
			ExpError: "invalid message transformation found; expected 'spEnrichedSetPk:{option}' but got \"spEnrichedSetPk\"",
		},
		{
			Name:     "message_empty_option_spEnrichedSetPk",
			Message:  "spEnrichedSetPk:",
			ExpError: "invalid message transformation found; empty option for 'spEnrichedSetPk'",
		},
		{
			Name:     "message_no_option_spEnrichedFilter",
			Message:  "spEnrichedFilter:too:wrong",
			ExpError: "invalid message transformation found; expected 'spEnrichedFilter:{option}' but got \"spEnrichedFilter:too:wrong\"",
		},
		{
			Name:     "message_empty_option_spEnrichedFilter",
			Message:  "spEnrichedFilter:",
			ExpError: "invalid message transformation found; empty option for 'spEnrichedFilter'",
		},
		{
			Name:     "message_no_option_spEnrichedFilterContext",
			Message:  "spEnrichedFilterContext:too:wrong",
			ExpError: "invalid message transformation found; expected 'spEnrichedFilterContext:{option}' but got \"spEnrichedFilterContext:too:wrong\"",
		},
		{
			Name:     "message_empty_option_spEnrichedFilterContext",
			Message:  "spEnrichedFilterContext:",
			ExpError: "invalid message transformation found; empty option for 'spEnrichedFilterContext'",
		},
		{
			Name:     "message_no_option_spEnrichedFilterUnstructEvent",
			Message:  "spEnrichedFilterUnstructEvent:too:wrong",
			ExpError: "invalid message transformation found; expected 'spEnrichedFilterUnstructEvent:{option}' but got \"spEnrichedFilterUnstructEvent:too:wrong\"",
		},
		{
			Name:     "message_empty_option_spEnrichedFilterUnstructEvent",
			Message:  "spEnrichedFilterUnstructEvent:",
			ExpError: "invalid message transformation found; empty option for 'spEnrichedFilterUnstructEvent'",
		},
		{
			Name:     "message_no_option_lua",
			Message:  "lua",
			ExpError: "invalid message transformation found; expected 'lua:{option}' but got \"lua\"",
		},
		{
			Name:     "message_empty_option_lua",
			Message:  "lua:",
			ExpError: "invalid message transformation found; empty option for 'lua'",
		},
		{
			Name:     "message_no_option_js",
			Message:  "js",
			ExpError: "invalid message transformation found; expected 'js:{option}' but got \"js\"",
		},
		{
			Name:     "message_empty_option_js",
			Message:  "js:",
			ExpError: "invalid message transformation found; empty option for 'js'",
		},
		{
			Name:     "invalid_transformation_syntax_a",
			Message:  "spEnrichedToJson,",
			ExpError: "empty transformation found; please check the message transformation syntax",
		},
		{
			Name:     "invalid_transformation_syntax_b",
			Message:  ":",
			ExpError: "empty transformation found; please check the message transformation syntax",
		},
		{
			Name:     "invalid_transformation_syntax_c",
			Message:  ",",
			ExpError: "empty transformation found; please check the message transformation syntax",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			parsed, err := parseTransformations(tt.Message)
			assert.Nil(parsed)
			if err == nil {
				t.Fatalf("expected error; got nil")
			}
			assert.Equal(tt.ExpError, err.Error())
		})
	}
}

func TestGetTransformations_MissingLayerConfig(t *testing.T) {
	fixturesDir := "../../../config/test-fixtures"
	testCases := []struct {
		Filename      string
		TransMessage  string
		ExpectedError string
	}{
		{
			Filename:      "transform-invalid-layer-lua.hcl",
			TransMessage:  "lua:fun",
			ExpectedError: "missing configuration for the custom transformation layer specified: \"lua\"",
		},
		{
			Filename:      "transform-invalid-layer-js.hcl",
			TransMessage:  "js:fun",
			ExpectedError: "missing configuration for the custom transformation layer specified: \"js\"",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Filename, func(t *testing.T) {
			assert := assert.New(t)

			filename := filepath.Join(fixturesDir, tt.Filename)
			t.Setenv("STREAM_REPLICATOR_CONFIG_FILE", filename)

			c, err := config.NewConfig()
			assert.NotNil(c)
			if err != nil {
				t.Fatalf("function NewConfig failed with error: %q", err.Error())
			}

			assert.Equal(c.Data.Transform.Message, tt.TransMessage)

			transformation, err := GetTransformations(c)
			assert.Nil(transformation)
			assert.NotNil(err)
			assert.Equal(tt.ExpectedError, err.Error())
		})
	}
}

func TestGetTransformations_Builtins(t *testing.T) {
	testCases := []struct {
		Name        string
		Provider    configProvider
		ExpectedErr error
	}{
		{
			Name: "invalid_transform_message",
			Provider: &testConfigProvider{
				message: "tooWrong",
			},
			ExpectedErr: fmt.Errorf("invalid transformation found; expected one of 'spEnrichedToJson', 'spEnrichedSetPk', 'spEnrichedFilter', 'spEnrichedFilterContext', 'spEnrichedFilterUnstructEvent', 'lua', 'js' or 'none' but got \"tooWrong\""),
		},
		{
			Name: "spEnrichedToJson",
			Provider: &testConfigProvider{
				message: "spEnrichedToJson",
			},
			ExpectedErr: nil,
		},
		{
			Name: "spEnrichedSetPk",
			Provider: &testConfigProvider{
				message: "spEnrichedSetPk:app_id",
			},
			ExpectedErr: nil,
		},
		{
			Name: "spEnrichedFilter",
			Provider: &testConfigProvider{
				message: "spEnrichedFilter:app_id==xyz",
			},
			ExpectedErr: nil,
		},
		{
			Name: "spEnrichedFilterContext",
			Provider: &testConfigProvider{
				message: "spEnrichedFilterContext:contexts_x_x_x_1.yz==xyz",
			},
			ExpectedErr: nil,
		},
		{
			Name: "spEnrichedFilterUnstructEvent",
			Provider: &testConfigProvider{
				message: "spEnrichedFilterUnstructEvent:unstruct_event_x_x_x_1.yz==xyz",
			},
			ExpectedErr: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			applyFun, err := GetTransformations(tt.Provider)

			if tt.ExpectedErr != nil {
				assert.Equal(tt.ExpectedErr.Error(), err.Error())
				assert.Nil(applyFun)
			} else {
				assert.Nil(err)
				assert.NotNil(applyFun)
			}
		})
	}
}

func TestGetTransformations_Custom(t *testing.T) {
	testCases := []struct {
		Name        string
		Provider    configProvider
		ExpectedErr error
	}{
		{
			Name: "lua",
			Provider: &testConfigProvider{
				message:   "lua:fun",
				layerName: "lua",
				component: &testEngine{
					smokeTestErr: nil,
					mkFunction:   testTransformationFunction,
				},
			},
			ExpectedErr: nil,
		},
		{
			Name: "js",
			Provider: &testConfigProvider{
				message:   "js:fun",
				layerName: "js",
				component: &testEngine{
					smokeTestErr: nil,
					mkFunction:   testTransformationFunction,
				},
			},
			ExpectedErr: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			applyFun, err := GetTransformations(tt.Provider)

			if tt.ExpectedErr != nil {
				assert.Equal(tt.ExpectedErr.Error(), err.Error())
				assert.Nil(applyFun)
			} else {
				assert.Nil(err)
				assert.NotNil(applyFun)
			}
		})
	}
}

func TestLayerRegistry(t *testing.T) {
	assert := assert.New(t)

	registry, err := getLayerRegistry()
	assert.Nil(err)

	_, okLua := registry["lua"]
	assert.True(okLua)

	_, okJs := registry["js"]
	assert.True(okJs)
}

func TestMkEngineFunction(t *testing.T) {
	testCases := []struct {
		Name        string
		Provider    *testConfigProvider
		Unit        *transformationUnit
		Registry    layerRegistry
		ExpectedErr error
	}{
		{
			Name: "missing_layer_config",
			Provider: &testConfigProvider{
				layerName: "test",
				component: "irrelevant",
				err:       nil,
			},
			Unit: &transformationUnit{
				name:   "noTest",
				option: "testFun",
			},
			Registry:    map[string]config.Pluggable{},
			ExpectedErr: fmt.Errorf("missing configuration for the custom transformation layer specified: \"noTest\""),
		},
		{
			Name: "unknown_layer",
			Provider: &testConfigProvider{
				layerName: "test",
				component: "irrelevant",
				err:       nil,
			},
			Unit: &transformationUnit{
				name:   "test",
				option: "testFun",
			},
			Registry:    map[string]config.Pluggable{},
			ExpectedErr: fmt.Errorf("unknown transformation layer specified"),
		},
		{
			Name: "provider_error",
			Provider: &testConfigProvider{
				layerName: "test",
				component: nil,
				err:       fmt.Errorf("some error"),
			},
			Unit: &transformationUnit{
				name:   "test",
				option: "testFun",
			},
			Registry: map[string]config.Pluggable{
				"test": &testPluggable{},
			},
			ExpectedErr: fmt.Errorf("some error"),
		},
		{
			Name: "no_engine_component",
			Provider: &testConfigProvider{
				layerName: "test",
				component: "notAnEngine",
				err:       nil,
			},
			Unit: &transformationUnit{
				name:   "test",
				option: "testFun",
			},
			Registry: map[string]config.Pluggable{
				"test": &testPluggable{},
			},
			ExpectedErr: fmt.Errorf("could not interpret custom transformation configuration"),
		},
		{
			Name: "engine_smoke_test_error",
			Provider: &testConfigProvider{
				layerName: "test",
				component: &testEngine{
					smokeTestErr: fmt.Errorf("smoke error"),
					mkFunction:   testTransformationFunction,
				},
				err: nil,
			},
			Unit: &transformationUnit{
				name:   "test",
				option: "testFun",
			},
			Registry: map[string]config.Pluggable{
				"test": &testPluggable{},
			},
			ExpectedErr: fmt.Errorf("smoke error"),
		},
		{
			Name: "happy_path",
			Provider: &testConfigProvider{
				layerName: "test",
				component: &testEngine{
					smokeTestErr: nil,
					mkFunction:   testTransformationFunction,
				},
				err: nil,
			},
			Unit: &transformationUnit{
				name:   "test",
				option: "testFun",
			},
			Registry: map[string]config.Pluggable{
				"test": &testPluggable{},
			},
			ExpectedErr: nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)

			fun, err := mkEngineFunction(
				tt.Provider,
				tt.Unit,
				tt.Registry,
			)

			if tt.ExpectedErr != nil {
				assert.Equal(tt.ExpectedErr.Error(), err.Error())
				assert.Nil(fun)
			} else {
				assert.Nil(err)
				assert.NotNil(fun)
			}
		})
	}
}

// Helpers
type testConfigProvider struct {
	message   string
	layerName string
	component interface{}
	err       error
}

// *testConfigProvider implements configProvider
func (tc *testConfigProvider) ProvideTransformMessage() string {
	return tc.message
}

func (tc *testConfigProvider) ProvideTransformLayerName() string {
	return tc.layerName
}

func (tc *testConfigProvider) ProvideTransformComponent(p config.Pluggable) (interface{}, error) {
	return tc.component, tc.err
}

type testPluggable struct{}

// *testPluggable implements config.Pluggable
func (tp *testPluggable) ProvideDefault() (interface{}, error) {
	return "placeholder", nil
}

func (tp *testPluggable) Create(i interface{}) (interface{}, error) {
	return "placeholder", nil
}

type testEngine struct {
	smokeTestErr error
	mkFunction   transform.TransformationFunction
}

// *testEngine implements transform.Engine
func (te *testEngine) SmokeTest(funName string) error {
	return te.smokeTestErr
}

func (te *testEngine) MakeFunction(funName string) transform.TransformationFunction {
	return te.mkFunction
}

func testTransformationFunction(*models.Message, interface{}) (*models.Message, *models.Message, *models.Message, interface{}) {
	return nil, nil, nil, nil
}
