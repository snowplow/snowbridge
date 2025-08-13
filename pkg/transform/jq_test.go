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

package transform

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/snowplow/snowbridge/v3/pkg/models"
)

func TestJQRunFunction_SpMode_true(t *testing.T) {
	testCases := []struct {
		Scenario        string
		JQCommand       string
		InputMsg        *models.Message
		InputInterState any
		Expected        map[string]*models.Message
		ExpInterState   any
		Error           error
	}{
		{
			Scenario:  "test_timestamp_to_epochMillis",
			JQCommand: `{ foo: .collector_tstamp | epochMillis }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":1557499235972}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_timestamp_to_epochMillis_context",
			JQCommand: `{ sessionId: .contexts_com_snowplowanalytics_snowplow_client_session_1[0].firstEventTimestamp | epochMillis }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"sessionId":1730129267100}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_timestamp_to_epoch",
			JQCommand: `{ foo: .collector_tstamp | epoch }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":1557499235}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_timestamp_to_epoch_chained",
			JQCommand: `{ foo: .collector_tstamp | epoch | todateiso8601 }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"2019-05-10T14:40:35Z"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_hash_context-sha1-wo-salt",
			JQCommand: `{ agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("sha1"; "") }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"agentName":"3767ff5f27dff1fc1a8a8bbf3aa53a7170adbcbea0ab43b3"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_hash_context-sha1-with-salt",
			JQCommand: `{ agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("sha1"; "09a2d6b3ecd943aa8512df1f") }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"agentName":"5841e55de6c4486fa092f044a5189570dec421cb06652829"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_hash_context-sha256-wo-salt",
			JQCommand: `{ agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("sha256"; "") }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"agentName":"262ca08d9db38199ac487454bd4accb795a33297516b3cec"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_hash_context-sha256-with-salt",
			JQCommand: `{ agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("sha256"; "09a2d6b3ecd943aa8512df1f") }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"agentName":"23e37c9c9aaed4e592b306b291deb43fd197551048704d57"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_hash_context-md5-wo-salt",
			JQCommand: `{ agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("md5"; "") }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"agentName":"30368c83ff2751652f501b62d6b965794d3512177f301db1"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "test_hash_context-md5-with-salt",
			JQCommand: `{ agentName: .contexts_nl_basjes_yauaa_context_1[0].agentNameVersionMajor | hash("md5"; "09a2d6b3ecd943aa8512df1f") }`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"agentName":"8ac2e6ae6f31687f8f52ec3ae553d1dc78a591d31bfae508"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "happy_path",
			JQCommand: `{foo: .app_id}`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"test-data1"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "happy_path_with_Intermediate_state",
			JQCommand: `{foo: .app_id}`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: SpTsv1Parsed,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"test-data1"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "selecting_from_context",
			JQCommand: `{foo: .contexts_nl_basjes_yauaa_context_1[0].operatingSystemName}`,
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"Unknown"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jqConfig := &JQMapperConfig{
				JQCommand:    tt.JQCommand,
				RunTimeoutMs: 100,
				SpMode:       true,
			}

			transFun, err := jqMapperConfigFunction(jqConfig)
			assert.NotNil(transFun)
			if err != nil {
				t.Fatalf("failed to create transformation function with error: %q", err.Error())
			}

			s, f, e, i := transFun(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareJQ(t, s, tt.Expected["success"], "success")
			assertMessagesCompareJQ(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareJQ(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func TestJQRunFunction_SpMode_false(t *testing.T) {
	testCases := []struct {
		Scenario        string
		JQCommand       string
		InputMsg        *models.Message
		InputInterState any
		Expected        map[string]*models.Message
		ExpInterState   any
		Error           error
	}{
		{
			Scenario:  "happy_path",
			JQCommand: `{foo: .app_id}`,
			InputMsg: &models.Message{
				Data:         SnowplowJSON1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"foo":"test-data1"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario: "with_multiple_returns",
			JQCommand: `
{
    bar: .foo | ..
}`,
			InputMsg: &models.Message{
				Data:         []byte(`{"foo":[1,2,3]}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"bar":[1,2,3]}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario: "epochMillis_on_nullable",
			JQCommand: `
      { 
        explicit_null: .explicit | epochMillis,
        no_such_field: .nonexistent | epochMillis,
        non_null: .non_null
      }`,
			InputMsg: &models.Message{
				Data:         []byte(`{"explicit": null, "non_null": "hello"}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"non_null":"hello"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "epochMillis_custom_timelayout",
			JQCommand: `{ sessionId: .time | epochMillis("2006-01-02 15:04:05.999")}`,
			InputMsg: &models.Message{
				Data:         []byte(`{"time": "2024-10-28 15:27:47.100"}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"sessionId":1730129267100}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario: "epoch_on_nullable",
			JQCommand: `
      { 
        explicit_null: .explicit | epoch,
        no_such_field: .nonexistent | epoch,
        non_null: .non_null
      }`,
			InputMsg: &models.Message{
				Data:         []byte(`{"explicit": null, "non_null": "hello"}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"non_null":"hello"}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "remove_nulls_struct",
			JQCommand: ".",
			InputMsg: &models.Message{
				Data: []byte(`
        {
          "f1": "value1",
          "f2": 2,
          "f3": {
            "f5": null,
            "f6": "value6",
            "f7": {
              "f8": 100,
              "f9": null
             }
           },
          "f4": null
        }`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"f1":"value1","f2":2,"f3":{"f6":"value6","f7":{"f8":100}}}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "remove_nulls_arrays",
			JQCommand: ".",
			InputMsg: &models.Message{
				Data: []byte(`
          {
            "items": [
              {
                "f1": "value1",
                "f2": null,
                "f3": [
                  {
                    "f4": 1,
                    "f5": null
                  },
                  {
                    "f4": null,
                    "f5": 20
                  }
                ]
              }
            ]
          }`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`{"items":[{"f1":"value1","f3":[{"f4":1},{"f5":20}]}]}`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
		{
			Scenario:  "remove_nulls_arrays_empty_output",
			JQCommand: ".items",
			InputMsg: &models.Message{
				Data: []byte(`
 			{"items": [{}, [], null, {"nested": []}]}
          `),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success": {
					Data:         []byte(`[]`),
					PartitionKey: "some-key",
				},
				"filtered": nil,
				"failed":   nil,
			},
			ExpInterState: nil,
			Error:         nil,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jqConfig := &JQMapperConfig{
				JQCommand:    tt.JQCommand,
				RunTimeoutMs: 100,
				SpMode:       false,
			}

			transFun, err := jqMapperConfigFunction(jqConfig)
			assert.NotNil(transFun)
			if err != nil {
				t.Fatalf("failed to create transformation function with error: %q", err.Error())
			}

			s, f, e, i := transFun(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareJQ(t, s, tt.Expected["success"], "success")
			assertMessagesCompareJQ(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareJQ(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func TestJQRunFunction_errors(t *testing.T) {
	testCases := []struct {
		Scenario        string
		JQConfig        *JQMapperConfig
		InputMsg        *models.Message
		InputInterState any
		Expected        map[string]*models.Message
		ExpInterState   any
		Error           error
	}{
		{
			Scenario: "not_a_map_a",
			JQConfig: &JQMapperConfig{
				JQCommand:    `.`,
				RunTimeoutMs: 100,
				SpMode:       false,
			},
			InputMsg: &models.Message{
				Data:         []byte(`[]`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`[]`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("cannot unmarshal array into Go value of type map[string]interface {}"),
		},
		{
			Scenario: "not_a_map_b",
			JQConfig: &JQMapperConfig{
				JQCommand:    `.`,
				RunTimeoutMs: 100,
				SpMode:       false,
			},
			InputMsg: &models.Message{
				Data:         []byte(`a`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`a`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("invalid character 'a' looking for beginning of value"),
		},
		{
			Scenario: "not_snowplow_event_with_spMode_true",
			JQConfig: &JQMapperConfig{
				JQCommand:    `.`,
				RunTimeoutMs: 100,
				SpMode:       true,
			},
			InputMsg: &models.Message{
				Data:         []byte(`a`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`a`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("Cannot parse tsv event"),
		},
		{
			Scenario: "deadline_exceeded",
			JQConfig: &JQMapperConfig{
				JQCommand:    `{foo: .app_id}`,
				RunTimeoutMs: 0,
				SpMode:       true,
			},
			InputMsg: &models.Message{
				Data:         SnowplowTsv1,
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         SnowplowTsv1,
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("context deadline"),
		},
		{
			Scenario: "no_output",
			JQConfig: &JQMapperConfig{
				JQCommand:    `.foo[].value`,
				RunTimeoutMs: 100,
				SpMode:       false,
			},
			InputMsg: &models.Message{
				Data:         []byte(`{"foo": []}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`{"foo": []}`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("jq query got no output"),
		},
		{
			Scenario: "epochMillis_on_non_time_type",
			JQConfig: &JQMapperConfig{
				JQCommand:    `.str | epochMillis`,
				RunTimeoutMs: 100,
				SpMode:       false,
			},
			InputMsg: &models.Message{
				Data:         []byte(`{"str": "value"}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`{"str": "value"}`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("could not parse input - 'value' using provided time layout - '2006-01-02T15:04:05.999Z'"),
		},
		{
			Scenario: "epoch_on_non_time_type",
			JQConfig: &JQMapperConfig{
				JQCommand:    `.str | epoch`,
				RunTimeoutMs: 100,
				SpMode:       false,
			},
			InputMsg: &models.Message{
				Data:         []byte(`{"str": "value"}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`{"str": "value"}`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("could not parse input - 'value' using provided time layout - '2006-01-02T15:04:05.999Z'"),
		},
		{
			Scenario: "epochMillis_with_not_matching_timelayout",
			JQConfig: &JQMapperConfig{
				JQCommand:    `{ sessionId: .time | epochMillis("2006-01-02 15:04:05") }`,
				RunTimeoutMs: 100,
				SpMode:       false,
			},
			InputMsg: &models.Message{
				Data:         []byte(`{"time": "2024-10-28T15:27:47.100"}`),
				PartitionKey: "some-key",
			},
			InputInterState: nil,
			Expected: map[string]*models.Message{
				"success":  nil,
				"filtered": nil,
				"failed": {
					Data:         []byte(`{"time": "2024-10-28T15:27:47.100"}`),
					PartitionKey: "some-key",
				},
			},
			ExpInterState: nil,
			Error:         errors.New("could not parse input - '2024-10-28T15:27:47.100' using provided time layout - '2006-01-02 15:04:05'"),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			transFun, err := jqMapperConfigFunction(tt.JQConfig)
			assert.NotNil(transFun)
			if err != nil {
				t.Fatalf("failed to create transformation function with error: %q", err.Error())
			}

			s, f, e, i := transFun(tt.InputMsg, tt.InputInterState)

			if !reflect.DeepEqual(i, tt.ExpInterState) {
				t.Errorf("\nINTERMEDIATE_STATE:\nGOT:\n%s\nEXPECTED:\n%s",
					spew.Sdump(i),
					spew.Sdump(tt.ExpInterState))
			}

			if e == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if e != nil {
				gotErr := e.GetError()
				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", gotErr.Error())
				}

				if !strings.Contains(gotErr.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						gotErr.Error(),
						expErr.Error())
				}
			}

			assertMessagesCompareJQ(t, s, tt.Expected["success"], "success")
			assertMessagesCompareJQ(t, f, tt.Expected["filtered"], "filtered")
			assertMessagesCompareJQ(t, e, tt.Expected["failed"], "failed")
		})
	}
}

func TestJQMapperConfigFunction(t *testing.T) {
	testCases := []struct {
		Scenario  string
		JQCommand string
		Error     error
	}{
		{
			Scenario: "compile_error",
			JQCommand: `
{
    foo: something_undefined
}
`,
			Error: errors.New("error compiling jq query"),
		},
		{
			Scenario:  "parsing_error",
			JQCommand: `^`,
			Error:     errors.New(`error parsing jq command: unexpected token "^"`),
		},
	}

	for _, tt := range testCases {
		t.Run(tt.Scenario, func(t *testing.T) {
			assert := assert.New(t)

			jqCfg := &JQMapperConfig{
				JQCommand:    tt.JQCommand,
				RunTimeoutMs: 100,
				SpMode:       false,
			}

			transFun, err := jqMapperConfigFunction(jqCfg)

			if err == nil && tt.Error != nil {
				t.Fatalf("missed expected error")
			}

			if err != nil {
				assert.Nil(transFun)

				expErr := tt.Error
				if expErr == nil {
					t.Fatalf("got unexpected error: %s", err.Error())
				}

				if !strings.Contains(err.Error(), expErr.Error()) {
					t.Errorf("GOT_ERROR:\n%s\n does not contain\nEXPECTED_ERROR:\n%s",
						err.Error(),
						expErr.Error())
				}
			}
		})
	}
}

// Helper
func assertMessagesCompareJQ(t *testing.T, act, exp *models.Message, hint string) {
	t.Helper()
	ok := false
	headersOk := false
	switch {
	case act == nil:
		ok = exp == nil
	case exp == nil:
	default:
		pkOk := act.PartitionKey == exp.PartitionKey
		dataOk := reflect.DeepEqual(act.Data, exp.Data)
		cTimeOk := reflect.DeepEqual(act.TimeCreated, exp.TimeCreated)
		pTimeOk := reflect.DeepEqual(act.TimePulled, exp.TimePulled)
		tTimeOk := reflect.DeepEqual(act.TimeTransformed, exp.TimeTransformed)
		ackOk := reflect.DeepEqual(act.AckFunc, exp.AckFunc)
		headersOk = reflect.DeepEqual(act.HTTPHeaders, exp.HTTPHeaders)

		if pkOk && dataOk && cTimeOk && pTimeOk && tTimeOk && ackOk && headersOk {
			ok = true
		}
	}

	if !ok {
		// message.HTTPHeaders are not printed
		if headersOk == false && act != nil && exp != nil {
			t.Errorf("\nUnexpected HTTPHeaders:\nGOT:\n%s\nEXPECTED:\n%s\n",
				spew.Sdump(act.HTTPHeaders),
				spew.Sdump(exp.HTTPHeaders))
		} else {
			t.Errorf("\nGOT:\n%s\nEXPECTED:\n%s\n",
				spew.Sdump(act),
				spew.Sdump(exp))
		}
	}
}
